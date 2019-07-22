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

type extractorResponse struct {
	extractor Extractor
	isSuccess bool
}

var extractors []*Extractor

//Init sets up extractors and loaders
func Init(l *zap.SugaredLogger, conf *Configuration) {
	var extractorwg sync.WaitGroup
	ti := time.Now()

	extractorsAttempts := map[int]int{}

	exResponse := make(chan extractorResponse)

	interval := conf.Interval
	start := conf.QueryMax.Start
	finish := conf.QueryMax.Finish

	l.Info(conf.MaxConcurrent)
	lock := make(chan int, conf.MaxConcurrent)

	iterations := ((finish - start) / interval) + 1
	//iterations = int(math.Ceil(iterations))
	l.Info("Iterations: ", iterations)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()
	waitForSignal(ctx, l, ti)

	go func() {
		for res := range exResponse {
			if !res.isSuccess {
				m := fmt.Sprintf("FAILED extractor %d to %d ID: %d", res.extractor.QueryStart, res.extractor.QueryLimit, res.extractor.id)
				fmt.Println(m)
				l.Warnf(m)

				if extractorsAttempts[res.extractor.id] > 5 {
					cancel()
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
				lock <- 0
				go extract(ctx, l, conf, &ex, exResponse, lock, &extractorwg)
				extractors = append(extractors, &ex)
				extractorsAttempts[res.extractor.id] = extractorsAttempts[res.extractor.id] + 1
			} else {
				m := fmt.Sprintf("Extractor %d to %d finished", res.extractor.QueryStart, res.extractor.QueryLimit)
				l.Info(m)
				fmt.Println(m)
			}
		}
	}()

	for i := 0; i < iterations; i++ {
		lock <- 0

		init := start + (i * interval)
		end := init + interval
		m := fmt.Sprintf("Sending extractor from %d to %d ID: %d", init, end, i)
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
		go extract(ctx, l, conf, &ex, exResponse, lock, &extractorwg)
		extractors = append(extractors, &ex)

		// Giving the first extractor a head start
		if i == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}

	extractorwg.Wait()
	elapsedTime(l, ti)
}

func extract(ctx context.Context, l *zap.SugaredLogger, cn *Configuration, ex *Extractor, exResponse chan extractorResponse, lock chan int, wg *sync.WaitGroup) {
	isSuccessful := true

	defer deLock(lock, l, ex.QueryStart)
	defer wg.Done()

	logger := l
	var err error

	em, err := getElasticManager(l, cn)
	if err != nil {
		logger.Panic("Error create elastic manager ", err)
		panic(err)
	}
	defer em.Close()

	ex.ElasticManager = em

	go func() {
		for r := range em.Respchan {

			if r.BulkResponse.Errors {
				logger.Error("Bulk response reported errors")
			} else {
				succeeded := r.BulkResponse.Succeeded()

				lastSucceded := succeeded[len(succeeded)-1]
				logger.Infow(
					"WORKER_RESPONSE",
					"succeeded",
					r.Succedded,
					"indexed",
					r.Indexed,
					"failed",
					r.Failed,
					"Took",
					r.BulkResponse.Took,
					"extractorStarted",
					ex.QueryStart,
					"lastSucceeded",
					lastSucceded.Id,
				)

				li, err := strconv.Atoi(lastSucceded.Id)
				if err != nil {
					logger.Panic("Error turning ID into int")
				}
				if ex.LastIDAdded < li {
					ex.LastIDAdded = li
				}
			}
			if r.Failed > 0 {
				logger.Error("Failed records on bulk")
				println("Failed records on elastic bulk")
				ids := ""
				reasons := ""
				fr := r.BulkResponse.Failed()
				logger.Error(fr[0].Error.Reason)

				for _, it := range r.BulkResponse.Failed() {
					ids = ids + "," + it.Id
					reasons = reasons + " " + it.Error.Reason
				}
				logger.Debug("IDs with error ", ids)
				logger.Debug("Reasons: ", reasons)

				isSuccessful = false
			}
		}
	}()

	go func() {
		for e := range em.Errchan {
			m := fmt.Sprintf("For worker started on %d Got error from bulk", ex.QueryStart)
			println(m)
			logger.Error(m)
			logger.Panic(e)
			isSuccessful = false
		}
	}()

	err = ex.Start(ctx)
	if err != nil {
		logger.Errorf("Extractor %d error: %s", ex.QueryStart, err.Error())
		print(err.Error())
		isSuccessful = false

		exResponse <- extractorResponse{
			extractor: *ex,
			isSuccess: isSuccessful,
		}

		return
	}

	if !isSuccessful {
		exResponse <- extractorResponse{
			extractor: *ex,
			isSuccess: isSuccessful,
		}
		return
	}

	logger.Infof("Waiting for elastic manager to finish. Extractor %d", ex.QueryStart)
	em.WaitGroup.Wait()
	time.Sleep(200 * time.Millisecond)
	logger.Infof("Finished extractor %d waiting for loader to finish. Last ID: %d", ex.QueryStart, ex.LastIDAdded)

	exResponse <- extractorResponse{
		extractor: *ex,
		isSuccess: isSuccessful,
	}
}

func deLock(lock chan int, l *zap.SugaredLogger, queryInit int) {
	l.Warnf("DeLocking %d", queryInit)
	<-lock
}

func getElasticManager(l *zap.SugaredLogger, cn *Configuration) (*ElasticManager, error) {
	logger := l

	ctx := context.Background()

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

	err := es.Init(cn.ElasticHost, logger)
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
	go func() {
		select {
		case <-c:
			l.Warn("Received Interrupt Signal")
			for _, ex := range extractors {
				m := fmt.Sprintf("For worker started on %d Last compound UCI: %d", ex.QueryStart, ex.LastIDAdded)
				println(m)
				l.Warn(m)
			}
			elapsedTime(l, t)
			os.Exit(1)
		case <-ctx.Done():
			l.Error("Something went wrong")
			fmt.Println("Something went wrong")
		}
	}()
}

func elapsedTime(l *zap.SugaredLogger, t time.Time) {
	logger := l
	e := time.Since(t)
	m := fmt.Sprintf("Elapsed %s", e)
	println(m)
	logger.Infof(m)
}
