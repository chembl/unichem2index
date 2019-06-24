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
	id                     int
	ElasticManager         *ElasticManager
	Oraconn                string
	Query                  string
	QueryLimit, QueryStart int
	Logger                 *zap.SugaredLogger
	LastIdAdded            int
	db                     *sql.DB
	//CurrentCompound contains the current compound being added to the loader
	PreviousCompound Compound
	CurrentCompound  Compound
}

// Start extracting unichem data by querying Unichem's db
// and adds them into the index using a provided ElasticManager
func (ex *Extractor) Start() error {
	ex.PreviousCompound = Compound{UCI: ""}
	ex.CurrentCompound = Compound{UCI: ""}

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

	var queryTemplate = ex.Query

	query := fmt.Sprintf(queryTemplate, ex.QueryStart, ex.QueryLimit)

	var (
		UCI, standardInchi, standardInchiKey, smiles                           string
		srcCompoundId, srcID, srcNameLong, srcName, srcDescription, srcBaseUrl string
	)

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
		err := rows.Scan(
			&UCI,
			&standardInchi,
			&standardInchiKey,
			&smiles,
			&srcCompoundId,
			&srcID,
			&srcNameLong,
			&srcName,
			&srcDescription,
			&srcBaseUrl)
		if err != nil {
			logger.Error("Error reading line")
			return err
		}
		logger.Debugw(
			"Row:",
			"UCI", UCI,
			"standarInchi", standardInchi,
			"standarInchiKey", standardInchiKey,
			"smiles", smiles,
			"srcCompoundId", srcCompoundId,
			"srcID", srcID,
			"srcName", srcName,
		)

		c = Compound{
			UCI:              UCI,
			Inchi:            standardInchi,
			StandardInchiKey: standardInchiKey,
			Smiles:           smiles,
			CreatedAt:        time.Now(),
		}
		ex.CurrentCompound = c
		//ex.ElasticManager.AddToIndex(c)
		ex.addToIndex(CompoundSource{
			ID:          srcID,
			Name:        srcName,
			LongName:    srcNameLong,
			SourceID:    srcCompoundId,
			Description: srcDescription,
			BaseUrl:     srcBaseUrl,
		})
	}

	logger.Debugf("Sending last bulk for extractor started on %d", ex.QueryStart)
	ex.ElasticManager.SendCurrentBulk()
	return nil
}

func (ex *Extractor) addToIndex(source CompoundSource) {
	logger := ex.Logger

	logger.Debugf("Found UCI <%s> Source ID %s Name %s", ex.CurrentCompound.UCI, source.ID, source.Name)

	if ex.PreviousCompound.UCI == "" {
		ex.CurrentCompound.Sources = append(ex.CurrentCompound.Sources, source)
		ex.PreviousCompound = ex.CurrentCompound
		return
	}

	if ex.PreviousCompound.UCI == ex.CurrentCompound.UCI {
		logger.Debugf("Current %d UCI matches previous compound: %d", ex.CurrentCompound.UCI, ex.PreviousCompound.UCI)
		ex.PreviousCompound.Sources = append(ex.PreviousCompound.Sources, source)
	} else {
		logger.Debugf("New compound UCI <%s> adding previous one <%s> to index", ex.CurrentCompound.UCI, ex.PreviousCompound.UCI)
		ex.ElasticManager.AddToIndex(ex.PreviousCompound)

		ex.CurrentCompound.Sources = append(ex.CurrentCompound.Sources, source)
		ex.PreviousCompound = ex.CurrentCompound
	}
}
