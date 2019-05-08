package extractor

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	wg     sync.WaitGroup
)

//Init sets up exractors and loaders
func Init(l *zap.SugaredLogger, conf *Configuration) {
	ti := time.Now()

	var extractors []*Extractor

	for _, r := range conf.QueryRanges {
		if r.Finish <= 0 {
			l.Panic("QueryLimit must be a number higher than 0")
		}
		ex := Extractor{
			Oraconn:    conf.OracleConn,
			QueryStart: r.Start,
			QueryLimit: r.Finish,
			Logger:     l,
		}
		l.Infof("Sending extractor from %d to %d", r.Start, r.Finish)
		wg.Add(1)
		go sendExtractor(l, conf, &ex)
		extractors = append(extractors, &ex)

		time.Sleep(300 * time.Millisecond)
	}
	waitForSignal(l, ti, extractors)
	wg.Wait()
	elapsedTime(l, ti)
}

func sendExtractor(l *zap.SugaredLogger, cn *Configuration, ex *Extractor) {
	defer wg.Done()
	logger := l
	var err error

	em, err := getElasticManager(l, cn)
	if err != nil {
		logger.Error("Error create elastic manager ", err)
	}
	defer em.Close()

	ex.ElasticManager = em

	go func() {
		for r := range em.Respchan {

			if r.BulkResponse.Errors {
				logger.Error("Bulk response reported errors")
			} else {
				s := r.BulkResponse.Succeeded()

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
					s[len(s) - 1].Id,
				)
			}
			if r.Failed > 0 {
				logger.Error("Failed records on bulk")
				ids := ""
				reasons := ""
				fr :=  r.BulkResponse.Failed()
				logger.Error(fr[0].Error.Reason)

				for _, it := range r.BulkResponse.Failed() {
					ids = ids + "," + it.Id
					reasons = reasons + " " + it.Error.Reason
				}

				logger.Error("IDs with error ", ids)
				logger.Debug("Reasons: ", reasons)
			}
		}
	}()

	go func() {
		for e := range em.Errchan {
			logger.Panicf("For worker started on %d Got error from bulk %s", ex.QueryStart, e)
		}
	}()

	err = ex.Start()
	if err != nil {
		logger.Warn("For worker started on %d", ex.QueryStart)
		logger.Panic("Extractor error ", err)
	}
	if ex.CurrentCompound != nil {
		logger.Infof("Last compound %s of worker started whit %d", ex.CurrentCompound.UCI, ex.QueryStart)
	}
	logger.Infof("Finished worker started with UCI %d waiting for loader to finish", ex.QueryStart)
	em.WaitGroup.Wait()
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

func waitForSignal(l *zap.SugaredLogger, t time.Time, exs []*Extractor) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		l.Warn("Received Interrupt Signal")
		for _, ex := range exs {
			l.Warnf("For worker started on %d Last compound UCI: %s", ex.QueryStart, ex.CurrentCompound.UCI)
		}
		elapsedTime(l, t)
		os.Exit(1)
	}()
}

func elapsedTime(l *zap.SugaredLogger, t time.Time) {
	logger := l
	e := time.Since(t)
	logger.Infof("Elapsed %s", e)
}
