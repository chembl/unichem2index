package extractor

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

//Extractor connects to the given oracle string connection (Oraconn), fetch
//the unichem data and adds it into the index using the ElasticManager provided
type Extractor struct {
	ElasticManager         *ElasticManager
	Oraconn                string
	QueryLimit, QueryStart int
	//CurrentCompound contains the current compound being added to the loader
	CurrentCompound *Compound
	Logger          *zap.SugaredLogger

	db *sql.DB
}

// Start extracting unichem data by querying Unichem's db
// and adds them into the index using a provided ElasticManager
func (ex *Extractor) Start() error {
	logger := ex.Logger

	var err error

	logger.Infof("Fetching from:%d to %d", ex.QueryStart, ex.QueryLimit)

	ex.db, err = sql.Open("goracle", ex.Oraconn)
	if err != nil {
		logger.Error("Go oracle open ERROR ", err)
		return err
	}
	defer ex.db.Close()
	logger.Info("Success connecting to Oracle DB")

	err = ex.queryByOneWithSources()
	if err != nil {
		return err
	}

	return nil
}

func (ex *Extractor) queryByOneWithSources() error {
	logger := ex.Logger

	ctx := context.Background()

	var queryTemplate = `SELECT str.UCI, str.STANDARDINCHI, str.STANDARDINCHIKEY, xrf.SRC_COMPOUND_ID, xrf.NAME
	FROM (
	  SELECT xrf.UCI, xrf.SRC_COMPOUND_ID, so.NAME
	  FROM UC_XREF xrf, UC_SOURCE so
	  WHERE xrf.UCI >= %d
	  AND xrf.UCI <= %d ) xrf, UC_STRUCTURE str
	WHERE xrf.UCI = str.UCI
	`

	query := fmt.Sprintf(queryTemplate, ex.QueryStart, ex.QueryLimit)

	var UCI, standardInchi, standardInchiKey, srcCompoundID, name string
	logger.Debug("Query: ", query)

	rows, err := ex.db.QueryContext(ctx, query)
	if err != nil {
		logger.Error("Error running query ", err)
		return err
	}
	defer rows.Close()
	logger.Info("Success, got rows")
	var c Compound
	for rows.Next() {
		rows.Scan(&UCI, &standardInchi, &standardInchiKey, &srcCompoundID, &name)
		c = Compound{
			UCI:              UCI,
			Inchi:            standardInchi,
			StandardInchiKey: standardInchiKey,
			CreatedAt:        time.Now(),
		}
		ex.addToIndex(c, srcCompoundID, name)
	}
	ex.ElasticManager.AddToIndex(*ex.CurrentCompound)
	ex.ElasticManager.SendCurrentBulk()
	return nil
}

func (ex *Extractor) addToIndex(c Compound, sci, n string) {
	logger := ex.Logger

	logger.Debugf("Found UCI <%s> Source ID %s Name %s", c.UCI, sci, n)

	if ex.CurrentCompound == nil {
		c.Sources = append(c.Sources, CompoundSource{
			ID:   sci,
			Name: n,
		})
		ex.CurrentCompound = &c
		return
	}

	if ex.CurrentCompound.UCI == c.UCI {
		logger.Debug("UCI matches current compound: ", ex.CurrentCompound.UCI)
		ex.CurrentCompound.Sources = append(ex.CurrentCompound.Sources, CompoundSource{
			ID:   sci,
			Name: n,
		})
	} else {
		logger.Debugf("New compound UCI <%s> adding previous one <%s> to index", c.UCI, ex.CurrentCompound.UCI)
		ex.ElasticManager.AddToIndex(*ex.CurrentCompound)

		c.Sources = append(c.Sources, CompoundSource{
			ID:   sci,
			Name: n,
		})

		ex.CurrentCompound = &c
	}
}

//PrintLastCompound logs the latest compound, if any, to be send to the loader
func (ex *Extractor) PrintLastCompound() {
	if ex.CurrentCompound == nil {
		ex.Logger.Warn("No current compound")
	} else {
		ex.Logger.Warn("Last compound ", ex.CurrentCompound.UCI)
	}
}
