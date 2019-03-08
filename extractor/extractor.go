package extractor

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/chembl/unichem2index/loader"
	_ "gopkg.in/goracle.v2"

	"go.uber.org/zap"
)

var (
	elasticManager  *loader.ElasticManager
	currentCompound loader.Compound
	logger          *zap.SugaredLogger
)

// StartExtraction queries Unichem's db in order to extract the compounds
// and to add them into the index using an ElasticManager
func StartExtraction(em *loader.ElasticManager, l *zap.SugaredLogger, oraconn string, queryLimit int) error {
	logger = l

	elasticManager = em

	var (
		limit = queryLimit
		start int
	)

	var queryTemplate = `SELECT UCI, STANDARDINCHI, STANDARDINCHIKEY
	FROM (
	  SELECT A.*, rownum rn
	  FROM (
		SELECT UCI, STANDARDINCHI, STANDARDINCHIKEY
		FROM UC_STRUCTURE
		ORDER BY UCI
		) A
	  WHERE rownum < %d)
	WHERE rn >= %d
	`

	// var queryTemplate = `SELECT UCI, STANDARDINCHI, STANDARDINCHIKEY, SRC_COMPOUND_ID, NAME
	// FROM (
	//   SELECT A.*, rownum rn
	//   FROM (
	// 	SELECT str.UCI, str.STANDARDINCHI, str.STANDARDINCHIKEY, xrf.SRC_COMPOUND_ID, so.NAME
	// 	FROM UC_STRUCTURE str, UC_XREF xrf, UC_SOURCE so
	// 	WHERE xrf.UCI = str.UCI
	// 	AND so.SRC_ID = xrf.SRC_ID
	// 	ORDER BY UCI
	// 	) A
	//   WHERE rownum < %d)
	// WHERE rn >= %d
	// `

	db, err := sql.Open("goracle", oraconn)
	if err != nil {
		logger.Error("Go oracle open ERROR ", err)
		return err
	}
	defer db.Close()
	logger.Debug("Success connecting to Oracle DB")

	hasResults := true

	for hasResults {
		var UCI, standardInchi, standardInchiKey string

		query := fmt.Sprintf(queryTemplate, start+limit, start)

		logger.Infof("INIT extracting structures: Fetching rows %d to %d", start, start+limit)
		logger.Debug(query)
		rows, err := db.Query(query)
		if err != nil {
			logger.Error("Error running query ", err)
			return err
		}
		defer rows.Close()

		logger.Info("Got rows")
		fr := rows.Next()
		if fr {
			rows.Scan(&UCI, &standardInchi, &standardInchiKey)
			c := loader.Compound{
				UCI:              UCI,
				Inchi:            standardInchi,
				StandardInchiKey: standardInchiKey,
				CreatedAt:        time.Now(),
			}
			elasticManager.AddToIndex(c)
		} else {
			hasResults = false
			logger.Warnf("No rows found from %d and %d", start, start+limit)
		}

		for rows.Next() {
			rows.Scan(&UCI, &standardInchi, &standardInchiKey)
			c := loader.Compound{
				UCI:              UCI,
				Inchi:            standardInchi,
				StandardInchiKey: standardInchiKey,
				CreatedAt:        time.Now(),
			}
			elasticManager.AddToIndex(c)
		}

		logger.Infof("END extracting structures: Loaded rows %d to %d", start, start+limit)
		start = start + limit
	}

	return nil
}

func fetchSources(db *sql.DB, c loader.Compound) error {
	var srcCompoundID, name string
	c.Sources = make([]loader.CompoundSource, 0)

	sourcesQuery := `SELECT so.NAME, xr.SRC_COMPOUND_ID
	FROM UC_XREF xr, UC_SOURCE so
	WHERE UCI = '%s'
	AND xr.SRC_ID = so.SRC_ID`

	query := fmt.Sprintf(sourcesQuery, c.UCI)
	logger.Debug("Quering sources for ", c.UCI)
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&name, &srcCompoundID)
		c.Sources = append(c.Sources, loader.CompoundSource{
			ID:   srcCompoundID,
			Name: name,
		})
	}

	elasticManager.AddToIndex(c)

	return nil
}

func addToIndex(UCI, si, sik, sci, n string) {
	logger.Debugf("Found UCI <%s> Source ID %s Name %s", UCI, sci, n)

	if currentCompound.UCI == "" {
		currentCompound = loader.Compound{
			UCI:              UCI,
			Inchi:            si,
			StandardInchiKey: sik,
			Sources: []loader.CompoundSource{
				loader.CompoundSource{
					ID:   sci,
					Name: n,
				},
			},
			CreatedAt: time.Now(),
		}
		return
	}

	if currentCompound.UCI == UCI {
		logger.Debug("UCI matches current compound: ", currentCompound.UCI)
		currentCompound.Sources = append(currentCompound.Sources, loader.CompoundSource{
			ID:   sci,
			Name: n,
		})
	} else {
		logger.Debugf("New compound UCI <%s> adding previous one <%s> to index", UCI, currentCompound.UCI)
		elasticManager.AddToIndex(currentCompound)

		currentCompound = loader.Compound{
			UCI:              UCI,
			Inchi:            si,
			StandardInchiKey: sik,
			Sources: []loader.CompoundSource{
				loader.CompoundSource{
					ID:   sci,
					Name: n,
				},
			},
			CreatedAt: time.Now(),
		}
	}
}
