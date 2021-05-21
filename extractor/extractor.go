package extractor

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
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

	var (
		UCI, srcID, assignment                                                        int
		standardInchi, standardInchiKey, smiles                                       string
		srcCompoundID, srcNameLong, srcName, srcDescription, srcBaseURL, srcShortName string
		srcBaseIDURLAvailable, srcAuxForURL                                           bool
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
			&srcID,
			&srcNameLong,
			&srcName,
			&srcDescription,
			&srcBaseURL,
			&srcShortName,
			&srcBaseIDURLAvailable,
			&srcAuxForURL)
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
			logger.Warnf("Compound (%d) without InChI key, skipping split", UCI)
		} else {
			splitInchi, err := ex.splitInchi(standardInchi)
			if err != nil {
				logger.Errorf("Split InChI error in UCI: %d", UCI)
				return err
			}

			i = Inchi{
				Version:               splitInchi["version"],
				Formula:               splitInchi["formula"],
				ConnectionsReg:        splitInchi["connections"],
				HAtomsReg:             splitInchi["hAtoms"],
				ChargeReg:             splitInchi["charge"],
				ProtonsReg:            splitInchi["protons"],
				StereoDbondReg:        splitInchi["stereoDbond"],
				StereoSP3Reg:          splitInchi["stereoSP3"],
				StereoSP3invertedReg:  splitInchi["stereoSP3inverted"],
				StereoTypeReg:         splitInchi["stereoType"],
				IsotopicAtoms:         splitInchi["isotopicAtoms"],
				IsotopicExchangeableH: splitInchi["isotopicExchangeableH"],
				Inchi:                 standardInchi,
			}
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
			BaseIDURLAvailable: srcBaseIDURLAvailable,
			AuxForURL:          srcAuxForURL,
			CreatedAt:          created,
			LastUpdate:         l,
		}, assignment)

	}

	if ex.PreviousCompound.UCI != 0 {
		ex.ElasticManager.AddToBulk(ex.PreviousCompound)
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
		if len(ex.PreviousCompound.Sources) <= 0 {
			logger.Debug("Compound with empty sources", ex.PreviousCompound.Sources)
			ex.PreviousCompound.IsSourceless = true
		}
		ex.ElasticManager.AddToBulk(ex.PreviousCompound)

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

func (ex *Extractor) splitInchi(inchi string) (map[string]string, error) {
	//logger := ex.Logger
	splitted := make(map[string]string)

	connectionsReg := `(?:/c(?P<connections>[^/]*))?`
	hAtomsReg := `(?:/h(?P<hAtoms>[^/]*))?`
	chargeReg := `(?:/q(?P<charge>[^/]*))?`
	protonsReg := `(?:/p(?P<protons>[^/]*))?`
	stereoDbondReg := `(?:/b(?P<stereoDbond>[^/]*))?`
	stereoSP3Reg := `(?:/t(?P<stereoSP3>[^/]*))?`
	stereoSP3invertedReg := `(?:/m(?P<stereoSP3inverted>[^/]*))?`
	stereoTypeReg := `(?:/s(?P<stereoType>\d))?`
	isotopicAtoms := `(?:/i(?P<isotopicAtoms>[^/]*))?`
	isotopicExchangeableH := `(?:/h(?P<isotopicExchangeableH>[^/]*))?`

	inchiReg := `^InChI=(?P<version>[^/]*)/(?P<formula>[^/]*)` + connectionsReg + hAtomsReg + chargeReg + protonsReg + stereoDbondReg + stereoSP3Reg + stereoSP3invertedReg + stereoTypeReg + isotopicAtoms + isotopicExchangeableH

	r := regexp.MustCompile(inchiReg)

	res := r.FindStringSubmatch(inchi)
	if res == nil {
		return splitted, fmt.Errorf("bad inchi format: %s", inchi)
	}

	for i, name := range r.SubexpNames() {
		if i != 0 {
			splitted[name] = res[i]
		}
	}
	//logger.Debug("Map", splitted)
	return splitted, nil
}
