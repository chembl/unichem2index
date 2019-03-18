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
func StartExtraction(em *loader.ElasticManager, l *zap.SugaredLogger, oraconn string, queryLimit int, queryStart int) error {
	logger = l

	elasticManager = em

	var (
		limit = queryLimit
		start = queryStart
	)

	logger.Infof("Start: %d Limit %d", start, limit)

	db, err := sql.Open("goracle", oraconn)
	if err != nil {
		logger.Error("Go oracle open ERROR ", err)
		return err
	}
	defer db.Close()
	logger.Debug("Success connecting to Oracle DB")

	// err = queryInBUlk(db, start, limit)
	// if err != nil {
	// 	return nil
	// }

	err = queryByOne(db, start)
	if err != nil {
		return nil
	}

	return nil
}

func queryByOne(db *sql.DB, start int) error {
	var queryTemplate = `SELECT UCI, STANDARDINCHI, STANDARDINCHIKEY
	FROM UC_STRUCTURE
	WHERE UCI > %d
	`
	query := fmt.Sprintf(queryTemplate, start)
	var UCI, standardInchi, standardInchiKey string

	logger.Info("INIT extracting structures")
	logger.Debug(query)
	rows, err := db.Query(query)
	if err != nil {
		logger.Error("Error running query ", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&UCI, &standardInchi, &standardInchiKey)
		c := loader.Compound{
			UCI:              UCI,
			Inchi:            standardInchi,
			StandardInchiKey: standardInchiKey,
			CreatedAt:        time.Now(),
		}
		// err := fetchSources(db, c)
		// if err != nil {
		// 	return nil
		// }
		elasticManager.AddToIndex(c)
	}
	elasticManager.SendCurrentBulk()
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

func queryInBUlk(db *sql.DB, start, limit int) error {
	var queryTemplate = `SELECT UCI, STANDARDINCHI, STANDARDINCHIKEY
	FROM (
	  SELECT A.*, rownum rn
	  FROM (
		SELECT UCI, STANDARDINCHI, STANDARDINCHIKEY
		FROM UC_STRUCTURE
		) A
	  WHERE rownum < %d)
	WHERE rn >= %d
	`

	for {
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

		logger.Info("Finished query")
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
			logger.Warnf("No rows found from %d and %d", start, start+limit)
			elasticManager.SendCurrentBulk()
			break
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
