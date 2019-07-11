package extractor

import (
	"context"
	"fmt"
	"github.com/gosuri/uiprogress"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

type extractorUpdate struct {
	id          int
	queryStart  int
	lastIndexed int
}

type extractorResponse struct {
	extractor Extractor
	isSuccess bool
}

var extractors []*Extractor

//Init sets up extractors and loaders
func Init(l *zap.SugaredLogger, conf *Configuration) {
	var extractorwg sync.WaitGroup
	ti := time.Now()
	uiprogress.Start()
	exUpdate := make(chan extractorUpdate)
	exResponse := make(chan extractorResponse)


	var bars []*uiprogress.Bar

	interval := conf.Interval
	start := conf.QueryMax.Start
	finish := conf.QueryMax.Finish

	l.Info(conf.MaxConcurrent)
	lock := make(chan int, conf.MaxConcurrent)

	iterations := ((finish - start) / interval) + 1
	//iterations = int(math.Ceil(iterations))
	l.Info("Iterations: ", iterations)

	waitForSignal(l, ti)
	go func() {
		for res := range exUpdate {
			err := bars[res.id].Set(res.lastIndexed)
			if err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		for res := range exResponse {
			if !res.isSuccess {
				lock <- 0
				l.Warnf("Sending failed extractor %d to %d", res.extractor.QueryStart, res.extractor.QueryLimit)
				ex := Extractor{
					id:          res.extractor.id,
					Oraconn:     conf.OracleConn,
					Query:       conf.Query,
					QueryStart:  res.extractor.QueryStart,
					QueryLimit:  res.extractor.QueryLimit,
					Logger:      l,
					LastIdAdded: 0,
				}
				extractorwg.Add(1)
				go extract(l, conf, &ex, exUpdate, exResponse, lock, &extractorwg)
				extractors = append(extractors, &ex)
			}
		}
	}()

	for i := 0; i < iterations; i++ {
		lock <- 0

		init := start + (i * interval)
		end := init + interval
		l.Infof("Sending extractor from %d to %d", init, end)
		ex := Extractor{
			id:          i,
			Oraconn:     conf.OracleConn,
			Query:       conf.Query,
			QueryStart:  init,
			QueryLimit:  end,
			Logger:      l,
			LastIdAdded: 0,
		}

		bar := uiprogress.AddBar(end).AppendCompleted()
		bar.PrependFunc(func(b *uiprogress.Bar) string {
			return fmt.Sprintf("Last %d", ex.LastIdAdded)
		})

		bar.AppendFunc(func(b *uiprogress.Bar) string {
			return fmt.Sprintf("%d-%d", ex.QueryStart, ex.QueryLimit)
		})
		bars = append(bars, bar)

		extractorwg.Add(1)
		go extract(l, conf, &ex, exUpdate, exResponse, lock, &extractorwg)
		extractors = append(extractors, &ex)
	}

	extractorwg.Wait()
	elapsedTime(l, ti)
}

func extract(l *zap.SugaredLogger, cn *Configuration, ex *Extractor, exUpdate chan extractorUpdate, exResponse chan extractorResponse, lock chan int, wg *sync.WaitGroup) {
	isSuccessful := true

	defer deLock(lock, l)
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
				if ex.LastIdAdded < li {
					ex.LastIdAdded = li
				}
				exUpdate <- extractorUpdate{
					id:          ex.id,
					queryStart:  ex.QueryStart,
					lastIndexed: ex.LastIdAdded,
				}
			}
			if r.Failed > 0 {
				logger.Error("Failed records on bulk")
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
			fmt.Println(m)
			logger.Error(m)
			logger.Panic(e)
			isSuccessful = false
		}
	}()

	err = ex.Start()
	if err != nil {
		logger.Warnf("For worker started on %d.", ex.QueryStart)
		logger.Panic("Extractor error ", err)
		isSuccessful = false
	}

	logger.Info("Waiting for EM waitgroup!!!")
	em.WaitGroup.Wait()
	time.Sleep(200 * time.Millisecond)
	logger.Infof("Finished worker started with UCI %d waiting for loader to finish. Last ID: %d", ex.QueryStart, ex.LastIdAdded)

	exResponse <- extractorResponse{
		extractor: *ex,
		isSuccess: isSuccessful,
	}
}

func deLock(lock chan int, l *zap.SugaredLogger) {
	l.Warn("DeLocking")
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

func waitForSignal(l *zap.SugaredLogger, t time.Time) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	go func() {
		<-c
		l.Warn("Received Interrupt Signal")
		for _, ex := range extractors {
			m := fmt.Sprintf("For worker started on %d Last compound UCI: %d", ex.QueryStart, ex.LastIdAdded)
			fmt.Println(m)
			l.Warn(m)
		}
		elapsedTime(l, t)
		os.Exit(1)
	}()
}

func elapsedTime(l *zap.SugaredLogger, t time.Time) {
	logger := l
	e := time.Since(t)
	m := fmt.Sprintf("Elapsed %s", e)
	fmt.Println(m)
	logger.Infof(m)
}
