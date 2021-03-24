package extractor

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

type extractionResponse struct {
	extractor Extractor
	isSuccess bool
}

//Init sets up extractors and loaders
func Init(l *zap.SugaredLogger, conf *Configuration, isUpdate bool) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	waitForSignal(cancel, l)

	if isUpdate {
		updateFromLastUCI(ctx, l, conf)
		updateRemovedSources(ctx, l, conf)
		return
	}
	startExtraction(ctx, l, conf)
}

func updateFromLastUCI(ctx context.Context, l *zap.SugaredLogger, conf *Configuration) {
	var queryRange = 10000000
	m := "STARTING UPDATING PROCESS"
	fmt.Println(m)
	l.Info(m)
	em, err := getElasticManager(ctx, l, conf)
	if err != nil {
		m := fmt.Sprint("Error creating elastic manager ", err)
		fmt.Println(m)
		l.Panic(m)
		panic(err)
	}

	lastUCI, err := em.getLastIndexedUCI()
	if err != nil {
		l.Panic(err)
	}
	em.Close()

	conf.QueryMax.Start = lastUCI - 1
	conf.QueryMax.Finish = lastUCI + queryRange
	startExtraction(ctx, l, conf)
}

func updateRemovedSources(ctx context.Context, l *zap.SugaredLogger, conf *Configuration) {
	m := "Updating Removed Sources"
	fmt.Println(m)
	l.Info(m)
	em, err := getElasticManager(ctx, l, conf)
	if err != nil {
		m := fmt.Sprint("Error creating elastic manager ", err)
		fmt.Println(m)
		l.Panic(m)
		panic(err)
	}

	lastUpdatedDate, err := em.getLastUpdated()
	if err != nil {
		m := fmt.Sprint("Error getting last updated ", err)
		fmt.Println(m)
		l.Panic(m)
	}
	em.Close()


	var queryTemplate = `SELECT
  ucpa.UCI,
  ucpa.STANDARDINCHI,
  ucpa.STANDARDINCHIKEY,
  ucpa.PARENT_SMILES,
  xref.SRC_COMPOUND_ID,
  xref.ASSIGNMENT,
  xref.CREATED,
  xref.LASTUPDATED,
  so.src_id,
  so.NAME_LONG,
  so.NAME_LABEL,
  so.DESCRIPTION,
  so.BASE_ID_URL,
  so.NAME,
  so.BASE_ID_URL_AVAILABLE,
  so.AUX_FOR_URL
FROM
(
    SELECT *
    FROM UC_XREF
    WHERE LASTUPDATED IS NOT NULL
    AND LASTUPDATED >= %s
) xreflu,
UC_XREF xref,
UC_SOURCE so,
(
  SELECT UCI, STANDARDINCHI, STANDARDINCHIKEY, PARENT_SMILES
  FROM UC_STRUCTURE
) ucpa
WHERE xref.UCI = ucpa.UCI
AND xref.UCI = xreflu.UCI
AND xref.src_id = so.src_id
ORDER BY ucpa.UCI`
	sd := lastUpdatedDate.AddDate(0,0,-15)
	fd := fmt.Sprintf(`TO_DATE('%s', 'YYYYMMDD')`, sd.Format("20060102"))
	query := fmt.Sprintf(queryTemplate, fd)
	l.Info(query)

	extractOne(ctx, l, conf, query)
}

func extractOne(ctx context.Context, l *zap.SugaredLogger, conf *Configuration, query string) {
	var extractors []*Extractor

	l.Info("Starting One extractor")
	ti := time.Now()
	var extractorwg sync.WaitGroup
	exResponse := make(chan extractionResponse)
	extractorsAttempts := map[int]int{}

	l.Infof("MaxConcurrent set: %d", conf.MaxConcurrent)
	lock := make(chan int, conf.MaxConcurrent)
	monitorExtraction(ctx, l, conf, lock, &extractorwg, exResponse, &extractorsAttempts, &extractors)

	if conf.MaxAttempts <= 0 {
		l.Panic("Maximum number of extractor attempts must be defined and greater than zero")
		return
	}
	l.Info("MaxAttempts: ", conf.MaxAttempts)

	ex := Extractor{
		id:          1,
		Oraconn:     conf.OracleConn,
		Query:       query,
		Logger:      l,
		LastIDAdded: 0,
	}

	extractorsAttempts[1] = 1

	extractorwg.Add(1)
	launchExtractor(ctx, l, conf, &ex, exResponse, lock, &extractorwg)

	extractorwg.Wait()
	l.Info("Wrapping it up")
	elapsedTime(l, ti)
}

func monitorExtraction(ctx context.Context, l *zap.SugaredLogger, conf *Configuration, lock chan int, extractorwg *sync.WaitGroup, exResponse chan extractionResponse, exAt *map[int]int, extractors *[]*Extractor) {
	ctx, cancel := context.WithCancel(ctx)
	extractorsAttempts := *exAt
	go func() {
		for {
			select {
			case res := <-exResponse:
				if !res.isSuccess {
					m := fmt.Sprintf("FAILED extractor ID: %d - %d to %d ", res.extractor.id, res.extractor.QueryStart, res.extractor.QueryLimit)
					fmt.Println(m)
					l.Warnf(m)

					if extractorsAttempts[res.extractor.id] >= conf.MaxAttempts {
						m := fmt.Sprintf("CRITICAL Extractor ID: %d Maximum amount of attemps %d reached extractor", res.extractor.id, extractorsAttempts[res.extractor.id])
						fmt.Println(m)
						l.Error(m)
						cancel()
						break
					}

					query := fmt.Sprintf(conf.Query, res.extractor.QueryStart, res.extractor.QueryLimit)
					ex := Extractor{
						id:          res.extractor.id,
						Oraconn:     conf.OracleConn,
						Query:       query,
						QueryStart:  res.extractor.QueryStart,
						QueryLimit:  res.extractor.QueryLimit,
						Logger:      l,
						LastIDAdded: 0,
					}
					extractorwg.Add(1)
					go launchExtractor(ctx, l, conf, &ex, exResponse, lock, extractorwg)
					*extractors = append(*extractors, &ex)
					extractorsAttempts[res.extractor.id] = extractorsAttempts[res.extractor.id] + 1

					m = fmt.Sprintf("ATTEMPT %d Extractor ID: %d", extractorsAttempts[res.extractor.id], res.extractor.id)
					println(m)
					l.Warn(m)
				} else {
					m := fmt.Sprintf("DONE Extractor ID: %d - %d to %d finished", res.extractor.id, res.extractor.QueryStart, res.extractor.QueryLimit)
					l.Info(m)
					fmt.Println(m)
				}
			case <-ctx.Done():
				m := "Canceled extractors response listener because of context done"
				l.Warn(m)
				println(m)

				return
			}
		}
	}()
}

func startExtraction(ctx context.Context, l *zap.SugaredLogger, conf *Configuration) {
	ti := time.Now()
	var extractors []*Extractor

	interval := conf.Interval
	start := conf.QueryMax.Start
	finish := conf.QueryMax.Finish
	var extractorwg sync.WaitGroup
	exResponse := make(chan extractionResponse)
	extractorsAttempts := map[int]int{}

	l.Infof("MaxConcurrent set: %d", conf.MaxConcurrent)
	lock := make(chan int, conf.MaxConcurrent)
	monitorExtraction(ctx, l, conf, lock, &extractorwg, exResponse, &extractorsAttempts, &extractors)

	if conf.MaxAttempts <= 0 {
		l.Panic("Maximum number of extractor attempts must be defined and greater than zero")
	}
	l.Info("MaxAttempts: ", conf.MaxAttempts)

	iterations := ((finish - start) / interval) + 1
	l.Info("Iterations: ", iterations)

	for i := 0; i < iterations; i++ {

		init := start + (i * interval)
		end := init + interval
		m := fmt.Sprintf("Dispatching Extractor ID: %d from %d to %d ", i, init, end)
		l.Infof(m)
		println(m)
		query := fmt.Sprintf(conf.Query, init, end)
		ex := Extractor{
			id:          i,
			Oraconn:     conf.OracleConn,
			Query:       query,
			QueryStart:  init,
			QueryLimit:  end,
			Logger:      l,
			LastIDAdded: 0,
		}

		extractorsAttempts[i] = 1

		extractorwg.Add(1)
		go launchExtractor(ctx, l, conf, &ex, exResponse, lock, &extractorwg)
		extractors = append(extractors, &ex)

		// Giving the first extractor a head start
		if i == 0 {
			time.Sleep(300 * time.Millisecond)
		}
	}

	extractorwg.Wait()
	l.Info("Wrapping it up")
	printStatus(l, extractors)
	elapsedTime(l, ti)
}

func launchExtractor(ctx context.Context, l *zap.SugaredLogger, cn *Configuration, ex *Extractor, exResponse chan extractionResponse, lock chan int, wg *sync.WaitGroup) {
	lock <- 0
	m := fmt.Sprintf("STARTED Extractor ID: %d from %d to %d", ex.id, ex.QueryStart, ex.QueryLimit)
	l.Infof(m)
	println(m)

	defer deLock(lock, l, ex.QueryStart, ex.id)
	defer wg.Done()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := l
	var err error

	select {
	case <-ctx.Done():
		m := fmt.Sprintf("CANCELED Extractor ID:%d", ex.id)
		logger.Warn(m)
		cancel()
		return
	default:
	}

	em, err := getElasticManager(ctx, l, cn)
	if err != nil {
		logger.Panic("Error creating elastic manager ", err)
		panic(err)
	}
	defer em.Close()

	ex.ElasticManager = em

	exerror := make(chan error)
	inFinish := make(chan int)
	ex.inFinish = inFinish
	ex.exerror = exerror
	isExtractorDone := false
	go ex.Start(ctx)

d:
	for {
		select {
		case <-inFinish:
			m := fmt.Sprintf("Finishing database extraction %d last extracted: %d", ex.id, ex.PreviousCompound.UCI)
			l.Info(m)
			isExtractorDone = true
			if ex.PreviousCompound.UCI == 0 {
				break d
			}
			if isExtractorDone && em.totalDoneJobs >= em.totalSentJobs {
				break d
			}
		case err := <-exerror:
			m := fmt.Sprintf("Extractor ID: %d error", ex.id)
			logger.Error(m)
			fmt.Println(err.Error())
			exResponse <- extractionResponse{
				extractor: *ex,
				isSuccess: false,
			}
			return
		case <-ctx.Done():
			m := fmt.Sprintf("Context canceled Extractor ID:%d", ex.id)
			logger.Warn(m)
			cancel()
			return
		case esResponse := <-em.Respchan:
			l.Debugf("Got response, Extractor status: %t TOTAL DONE JOBS: %d TOTAL SENT JOBS: %d", isExtractorDone, em.totalDoneJobs, em.totalSentJobs)

			if esResponse.BulkResponse.Errors {
				logger.Error("Bulk response reported errors")
			} else {
				succeeded := esResponse.BulkResponse.Succeeded()

				lastSucceded := succeeded[len(succeeded)-1]
				logger.Infow(
					"WORKER_RESPONSE",
					"extractorID",
					ex.id,
					"extractorStarted",
					ex.QueryStart,
					"lastSucceeded",
					lastSucceded.Id,
					"Took",
					esResponse.BulkResponse.Took,
				)

				li, err := strconv.Atoi(lastSucceded.Id)
				if err != nil {
					logger.Panic("Error turning ID into int")
				}
				if ex.LastIDAdded < li {
					ex.LastIDAdded = li
				}
				em.totalDoneJobs++
			}
			if esResponse.Failed > 0 {
				m := fmt.Sprintf("Failed records on bulk. Extractor ID: %d", ex.id)
				logger.Error(m)

				ids := ""
				reasons := ""

				failed := esResponse.BulkResponse.Failed()
				lastFailed := failed[len(failed)-1]
				fr := esResponse.BulkResponse.Failed()
				logger.Error(fr[0].Error.Reason)
				logger.Errorw(
					"WORKER_ERROR",
					"extractorID",
					ex.id,
					"extractorStarted",
					ex.QueryStart,
					"Took",
					esResponse.BulkResponse.Took,
					"succeeded",
					esResponse.Succedded,
					"indexed",
					esResponse.Indexed,
					"failed",
					esResponse.Failed,
					"startedOn",
					failed[0].Id,
					"lastFailed",
					lastFailed.Id,
				)
				for _, it := range esResponse.BulkResponse.Failed() {
					ids = ids + "," + it.Id
					reasons = reasons + " " + it.Error.Reason
				}
				logger.Debug("IDs with error ", ids)
				logger.Debug("Reasons: ", reasons)

				exResponse <- extractionResponse{
					extractor: *ex,
					isSuccess: false,
				}
				cancel()
				return
			}
			if isExtractorDone && em.totalDoneJobs >= em.totalSentJobs {
				break d
			}
		case err = <-em.Errchan:
			m := fmt.Sprintf("For worker started on %d Got error from bulk", ex.QueryStart)
			println(m)
			logger.Error(m)
			println(err.Error())

			exResponse <- extractionResponse{
				extractor: *ex,
				isSuccess: false,
			}

			return
		}
	}
	logger.Infof("Waiting for elastic manager to finish. Extractor ID: %d", ex.id)
	em.WaitGroup.Wait()

	exResponse <- extractionResponse{
		extractor: *ex,
		isSuccess: true,
	}
}

func deLock(lock chan int, l *zap.SugaredLogger, queryInit int, exID int) {
	l.Warnf("DeLocking %d Extractor ID: %d", queryInit, exID)
	<-lock
}

func getElasticManager(ctx context.Context, l *zap.SugaredLogger, cn *Configuration) (*ElasticManager, error) {
	logger := l

	if cn.BulkLimit <= 0 {
		logger.Panic("BulkLimit must be a number higher than 0")
	}

	if cn.MaxBulkCalls <= 0 {
		logger.Panic("MaxBulkCalls must be a number higher than 0")
	}

	es := ElasticManager{
		Context:      ctx,
		IndexName:    "unichem",
		TypeName:     "compound",
		Bulklimit:    cn.BulkLimit,
		MaxBulkCalls: cn.MaxBulkCalls,
	}

	err := es.Init(ctx, cn, logger)
	if err != nil {
		logger.Error("Error init ElasticManager ", err)
		return nil, err
	}

	return &es, nil
}

func waitForSignal(cancel context.CancelFunc, l *zap.SugaredLogger) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	go func(cancel context.CancelFunc) {
		ossig := <-c
		l.Warnf("Received OS Signal: %+v", ossig)
		cancel()
		// os.Exit(1)
	}(cancel)
}

func elapsedTime(l *zap.SugaredLogger, t time.Time) {
	logger := l
	e := time.Since(t)
	m := fmt.Sprintf("Elapsed %s", e)
	println(m)
	logger.Infof(m)
}

func printStatus(l *zap.SugaredLogger, extractors []*Extractor) {
	for _, ex := range extractors {
		m := fmt.Sprintf("For worker started on %d Last compound UCI: %d", ex.QueryStart, ex.LastIDAdded)
		println(m)
		l.Warn(m)
	}
}
