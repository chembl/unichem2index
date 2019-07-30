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

var extractors []*Extractor

//Init sets up extractors and loaders
func Init(l *zap.SugaredLogger, conf *Configuration) {
	ti := time.Now()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()
	waitForSignal(ctx, l, ti)

	interval := conf.Interval
	start := conf.QueryMax.Start
	finish := conf.QueryMax.Finish

	extractorsAttempts := map[int]int{}

	l.Infof("MaxConcurrent set: %d", conf.MaxConcurrent)
	lock := make(chan int, conf.MaxConcurrent)

	iterations := ((finish - start) / interval) + 1
	l.Info("Iterations: ", iterations)

	var extractorwg sync.WaitGroup
	exResponse := make(chan extractionResponse)
	go func() {
		for {
			select {
			case res := <-exResponse:
				if !res.isSuccess {
					m := fmt.Sprintf("FAILED extractor %d to %d ID: %d", res.extractor.QueryStart, res.extractor.QueryLimit, res.extractor.id)
					fmt.Println(m)
					l.Warnf(m)

					if extractorsAttempts[res.extractor.id] >= 2 {
						m := fmt.Sprintf("Maximum amount of attemps %d reached extractor ID: %d", extractorsAttempts[res.extractor.id], res.extractor.id)
						fmt.Println(m)
						l.Error(m)
						cancel()
						break
					}

					ex := Extractor{
						id:          res.extractor.id,
						Oraconn:     conf.OracleConn,
						Query:       conf.Query,
						QueryStart:  res.extractor.QueryStart,
						QueryLimit:  res.extractor.QueryLimit,
						Logger:      l,
						LastIDAdded: 0,
					}
					extractorwg.Add(1)
					go startExtraction(ctx, l, conf, &ex, exResponse, lock, &extractorwg)
					extractors = append(extractors, &ex)
					extractorsAttempts[res.extractor.id] = extractorsAttempts[res.extractor.id] + 1

					m = fmt.Sprintf("ATTEMPT %d Extractor ID:%d", extractorsAttempts[res.extractor.id], res.extractor.id)
					println(m)
					l.Warn(m)
				} else {
					m := fmt.Sprintf("Extractor ID:%d - %d to %d finished", res.extractor.id, res.extractor.QueryStart, res.extractor.QueryLimit)
					l.Info(m)
					fmt.Println(m)
				}
			case <-ctx.Done():
				m := "Interrumping Extractor response listener because of context done"
				l.Warn(m)
				println(m)

				return
			}
		}
	}()

	for i := 0; i < iterations; i++ {

		init := start + (i * interval)
		end := init + interval
		m := fmt.Sprintf("Created extractor ID: %d from %d to %d ", i, init, end)
		l.Infof(m)
		println(m)
		ex := Extractor{
			id:          i,
			Oraconn:     conf.OracleConn,
			Query:       conf.Query,
			QueryStart:  init,
			QueryLimit:  end,
			Logger:      l,
			LastIDAdded: 0,
		}

		extractorsAttempts[i] = 1

		extractorwg.Add(1)
		go startExtraction(ctx, l, conf, &ex, exResponse, lock, &extractorwg)
		extractors = append(extractors, &ex)

		// Giving the first extractor a head start
		if i == 0 {
			time.Sleep(300 * time.Millisecond)
		}
	}

	extractorwg.Wait()
	elapsedTime(l, ti)
}

func startExtraction(ctx context.Context, l *zap.SugaredLogger, cn *Configuration, ex *Extractor, exResponse chan extractionResponse, lock chan int, wg *sync.WaitGroup) {
	lock <- 0
	m := fmt.Sprintf("Stared extractor from %d to %d ID: %d", ex.QueryStart, ex.QueryLimit, ex.id)
	l.Infof(m)
	println(m)

	defer deLock(lock, l, ex.QueryStart, ex.id)
	defer wg.Done()
	exctx := context.Background()
	exctx, cancel := context.WithCancel(exctx)
	defer cancel()

	logger := l
	var err error

	em, err := getElasticManager(exctx, l, cn)
	if err != nil {
		logger.Panic("Error create elastic manager ", err)
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
			fmt.Printf("Finishing extractor %d last extracted: %s \n", ex.id, ex.PreviousCompound.UCI)
			isExtractorDone = true
			if ex.PreviousCompound.UCI == "" {
				break d
			}
			if isExtractorDone && em.totalDoneJobs >= em.totalSentJobs {
				break d
			}
		case err := <-exerror:
			m := fmt.Sprintf("Extractor ID:%d error", ex.id)
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

	err := es.Init(ctx, cn.ElasticHost, logger)
	if err != nil {
		logger.Error("Error init ElasticManager ", err)
		return nil, err
	}

	return &es, nil
}

func waitForSignal(ctx context.Context, l *zap.SugaredLogger, t time.Time) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	go func(ctx context.Context) {
		ctx, cancel := context.WithCancel(ctx)
		ossig := <-c
		l.Warnf("Received OS Signal: %+v", ossig)
		for _, ex := range extractors {
			m := fmt.Sprintf("For worker started on %d Last compound UCI: %d", ex.QueryStart, ex.LastIDAdded)
			println(m)
			l.Warn(m)
		}
		elapsedTime(l, t)
		cancel()
		// os.Exit(1)
	}(ctx)
}

func elapsedTime(l *zap.SugaredLogger, t time.Time) {
	logger := l
	e := time.Since(t)
	m := fmt.Sprintf("Elapsed %s", e)
	println(m)
	logger.Infof(m)
}
