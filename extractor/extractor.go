package extractor

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/chembl/unichem-to-index/loader"
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
func StartExtraction(em *loader.ElasticManager, l *zap.SugaredLogger, oraconn string) error {
	logger = l

	elasticManager = em

	const (
		limit         = 50
		maxIterations = 2
	)
	var (
		start     = 0
		iteration = 0
	)

	var queryTemplate = `SELECT UCI, STANDARDINCHI, STANDARDINCHIKEY, SRC_COMPOUND_ID, NAME
	FROM (
	  SELECT A.*, rownum rn
	  FROM (
		SELECT str.UCI, str.STANDARDINCHI, str.STANDARDINCHIKEY, xrf.SRC_COMPOUND_ID, so.NAME
		FROM UC_STRUCTURE str, UC_XREF xrf, UC_SOURCE so
		WHERE xrf.UCI = str.UCI
		AND so.SRC_ID = xrf.SRC_ID
		ORDER BY UCI
		) A
	  WHERE rownum < %d)
	WHERE rn >= %d
	`

	db, err := sql.Open("goracle", oraconn)
	if err != nil {
		logger.Error("Go oracle open ERROR ", err)
		return err
	}
	defer db.Close()

	hasResults := true

	for hasResults {
		var (
			UCI, srcCompoundID, name        string
			standardInchi, standardInchiKey string
		)

		query := fmt.Sprintf(queryTemplate, start+limit, start)

		logger.Debug(query)

		rows, err := db.Query(query)
		if err != nil {
			logger.Error("Error running query ", err)
			return err
		}
		defer rows.Close()

		fr := rows.Next()
		if fr {
			rows.Scan(&UCI, &standardInchi, &standardInchiKey, &srcCompoundID, &name)
			addToIndex(UCI, standardInchi, standardInchiKey, srcCompoundID, name)
		} else {
			hasResults = false
			logger.Warn("Has results FALSE")
		}

		for rows.Next() {
			rows.Scan(&UCI, &standardInchi, &standardInchiKey, &srcCompoundID, &name)
			addToIndex(UCI, standardInchi, standardInchiKey, srcCompoundID, name)
		}

		start = start + limit

		if iteration >= maxIterations {
			break
		} else {
			logger.Info("Loaded first ", start)
			iteration++
		}
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
		elasticManager.SendToElastic(currentCompound, logger)

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
