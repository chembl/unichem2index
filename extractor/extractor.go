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

	var queryTemplate = `SELECT uc.UCI, uc.STANDARDINCHI, uc.STANDARDINCHIKEY, pa.PARENT_SMILES
       FROM UC_INCHI uc, SS_PARENTS pa
       WHERE uc.UCI >= %d
       AND uc.UCI < %d
       AND uc.UCI = pa.UCI`

	query := fmt.Sprintf(queryTemplate, ex.QueryStart, ex.QueryLimit)

	var UCI, standardInchi, standardInchiKey, smiles string
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
		err := rows.Scan(&UCI, &standardInchi, &standardInchiKey, &smiles)
		logger.Debugw("Row:", "UCI", UCI, "standarInchi", standardInchi, "standarInchiKey", standardInchiKey, "smiles", smiles)
		if err != nil {
			logger.Error("Error reading line")
			return err
		}
		c = Compound{
			UCI:              UCI,
			Inchi:            standardInchi,
			StandardInchiKey: standardInchiKey,
			Smiles:           smiles,
			CreatedAt:        time.Now(),
		}
		ex.CurrentCompound = &c
		ex.ElasticManager.AddToIndex(c)
		//time.Sleep(500 * time.Millisecond)
	}

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
