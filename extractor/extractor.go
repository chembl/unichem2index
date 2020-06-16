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
	LastIDAdded            int
	db                     *sql.DB
	exerror                chan error
	inFinish               chan int
	Attemps                int
	//CurrentCompound contains the current compound being added to the loader
	PreviousCompound Compound
	CurrentCompound  Compound
}

// Start extracting unichem data by querying Unichem's db
// and adds them into the index using a provided ElasticManager
func (ex *Extractor) Start(ctx context.Context) error {
	ex.PreviousCompound = Compound{UCI: ""}
	ex.CurrentCompound = Compound{UCI: ""}

	logger := ex.Logger

	var err error

	logger.Infof("Fetching from:%d to %d", ex.QueryStart, ex.QueryLimit)

	ex.db, err = sql.Open("godror", ex.Oraconn)
	if err != nil {
		ex.exerror <- err
		logger.Error("Go oracle open ERROR ", err)
		return err
	}
	defer ex.db.Close()
	logger.Info("Success connecting to Oracle DB")

	err = ex.queryByOneWithSources(ctx)
	if err != nil {
		ex.exerror <- err
		return err
	}

	return nil
}

func (ex *Extractor) queryByOneWithSources(ctx context.Context) error {
	logger := ex.Logger

	var queryTemplate = ex.Query

	query := fmt.Sprintf(queryTemplate, ex.QueryStart, ex.QueryLimit)

	var (
		UCI, standardInchi, standardInchiKey, smiles                           string
		srcCompoundID, srcID, srcNameLong, srcName, srcDescription, srcBaseURL string
	)

	logger.Debug("Query: ", query)

	rows, err := ex.db.QueryContext(ctx, query)
	if err != nil {
		logger.Error("Error running query ", err)
		return err
	}
	defer rows.Close()

	logger.Infof("Success, got rows from extractor %d started on %d", ex.id, ex.QueryStart)
	var c Compound
l:
	for rows.Next() {

		select {
		case <-ctx.Done():
			logger.Warnf("Interrumping extractor %d because of context done", ex.id)
			break l
		default:
		}

		err := rows.Scan(
			&UCI,
			&standardInchi,
			&standardInchiKey,
			&smiles,
			&srcCompoundID,
			&srcID,
			&srcNameLong,
			&srcName,
			&srcDescription,
			&srcBaseURL)
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
			"srcCompoundID", srcCompoundID,
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

		ex.addToIndex(CompoundSource{
			ID:          srcID,
			Name:        srcName,
			LongName:    srcNameLong,
			CompoundID:  srcCompoundID,
			Description: srcDescription,
			BaseURL:     srcBaseURL,
		})

	}

	if ex.PreviousCompound.UCI != "" {
		ex.ElasticManager.AddToIndex(ex.PreviousCompound)
	}

	logger.Infof("Sending last bulk for extractor started on %d", ex.QueryStart)
	ex.ElasticManager.SendCurrentBulk()

	ex.inFinish <- 1
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
		logger.Debugf("Current %s UCI matches previous compound: %s", ex.CurrentCompound.UCI, ex.PreviousCompound.UCI)
		ex.PreviousCompound.Sources = append(ex.PreviousCompound.Sources, source)
	} else {
		logger.Debugf("New compound UCI <%s> adding previous one <%s> to index", ex.CurrentCompound.UCI, ex.PreviousCompound.UCI)
		ex.ElasticManager.AddToIndex(ex.PreviousCompound)

		ex.CurrentCompound.Sources = append(ex.CurrentCompound.Sources, source)
		ex.PreviousCompound = ex.CurrentCompound
	}
}
