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

// Start extracting UniChem data by querying UniChem's db
// and adds them into the index using a provided ElasticManager
func (ex *Extractor) Start(ctx context.Context) error {
	ex.PreviousCompound = Compound{UCI: 0}
	ex.CurrentCompound = Compound{UCI: 0}

	logger := ex.Logger

	var err error

	logger.Infof("Fetching from:%d to %d", ex.QueryStart, ex.QueryLimit)

	ex.db, err = sql.Open("godror", ex.Oraconn)
	if err != nil {
		ex.exerror <- err
		logger.Error("Go oracle open ERROR ", err)
		return err
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			m := fmt.Sprint("Go oracle Closing DB ", err)
			fmt.Println(m)
			logger.Error(m)
		}
	}(ex.db)
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

	var (
		UCI, srcID, assignment, srcPrivate, srcBaseIDURLAvailable, srcAuxForURL       int
		standardInchi, standardInchiKey, smiles, auxSrc                               string
		srcCompoundID, srcNameLong, srcName, srcDescription, srcBaseURL, srcShortName string
		lastUpdated                                                                   sql.NullTime
		created                                                                       time.Time
	)

	logger.Debug("Query: ", ex.Query)

	rows, err := ex.db.QueryContext(ctx, ex.Query)
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
			&assignment,
			&created,
			&lastUpdated,
			&auxSrc,
			&srcID,
			&srcNameLong,
			&srcName,
			&srcDescription,
			&srcBaseURL,
			&srcShortName,
			&srcBaseIDURLAvailable,
			&srcAuxForURL,
			&srcPrivate)
		if err != nil {
			logger.Error(err, "Error reading line")
			return err
		}
		//logger.Debugw(
		//	"Row:",
		//	"UCI", UCI,
		//	"standarInchi", standardInchi,
		//	"standarInchiKey", standardInchiKey,
		//	"smiles", smiles,
		//	"srcCompoundID", srcCompoundID,
		//	"srcID", srcID,
		//	"srcName", srcName,
		//)

		i := *new(Inchi)
		if len(standardInchi) == 0 {
			logger.Debugf("Compound (%d) without InChI key, skipping split", UCI)
		} else {
			i.Inchi = standardInchi
		}

		isPrivate := false
		if srcPrivate == 1 {
			isPrivate = true
		}

		isBaseIDURLAvailable := false
		if srcBaseIDURLAvailable == 1 {
			isBaseIDURLAvailable = true
		}

		hasAuxForURL := false
		if srcAuxForURL == 1 {
			hasAuxForURL = true
		}
		c = Compound{
			UCI:              UCI,
			Inchi:            i,
			StandardInchiKey: standardInchiKey,
			Smiles:           smiles,
			CreatedAt:        time.Now(),
			IsSourceless:     false,
		}
		ex.CurrentCompound = c

		var l time.Time
		if lastUpdated.Valid {
			l = lastUpdated.Time
		}

		ex.addSourceToCompound(CompoundSource{
			ID:                 srcID,
			Name:               srcName,
			LongName:           srcNameLong,
			CompoundID:         srcCompoundID,
			Description:        srcDescription,
			BaseURL:            srcBaseURL,
			ShortName:          srcShortName,
			BaseIDURLAvailable: isBaseIDURLAvailable,
			AuxSrc:             auxSrc,
			AuxForURL:          hasAuxForURL,
			CreatedAt:          created,
			LastUpdate:         l,
			IsPrivate:          isPrivate,
		}, assignment)

	}

	if ex.PreviousCompound.UCI != 0 {
		ex.addPreviousCompoundToBulk()
	} else {
		logger.Warn("PREVIOUS COMPOUND EMPTY! Worker had nothing to work with")
	}

	logger.Infof("Sending last bulk for extractor started on %d", ex.QueryStart)
	ex.ElasticManager.SendCurrentBulk()

	ex.inFinish <- 1
	logger.Debug("1 to channel inFinish ")
	return nil
}

func (ex *Extractor) addSourceToCompound(source CompoundSource, assignment int) {
	logger := ex.Logger

	logger.Debugf("Found UCI <%d> Source ID %d Name %s", ex.CurrentCompound.UCI, source.ID, source.Name)

	if ex.PreviousCompound.UCI == 0 {
		if assignment == 1 {
			ex.CurrentCompound.Sources = append(ex.CurrentCompound.Sources, source)
		}
		ex.PreviousCompound = ex.CurrentCompound
		return
	}

	if ex.PreviousCompound.UCI != ex.CurrentCompound.UCI {
		logger.Debugf("New compound UCI <%d> adding previous one <%d> to index", ex.CurrentCompound.UCI, ex.PreviousCompound.UCI)
		ex.addPreviousCompoundToBulk()

		if assignment == 1 {
			ex.CurrentCompound.Sources = append(ex.CurrentCompound.Sources, source)
		}
		ex.PreviousCompound = ex.CurrentCompound
	} else {
		if assignment == 1 {
			ex.PreviousCompound.Sources = append(ex.PreviousCompound.Sources, source)
		}
	}
}

func (ex *Extractor) addPreviousCompoundToBulk() {
	logger := ex.Logger
	var err error
	if len(ex.PreviousCompound.Sources) <= 0 {
		logger.Debug("Compound with empty sources", ex.PreviousCompound.Sources)
		ex.PreviousCompound.IsSourceless = true
	}

	c := ex.PreviousCompound
	if len(c.Inchi.Inchi) > 1 {
		inDi := InchiDivider{logger}
		c, err = inDi.ProcessInchi(ex.PreviousCompound)
		if err != nil {
			m := fmt.Sprintf("Split InChI error in UCI: %d ", ex.PreviousCompound.UCI)
			fmt.Println(m, err)
			logger.Panic(m, err)
			panic(err)
		}
	}

	ex.ElasticManager.AddToBulk(c)
}
