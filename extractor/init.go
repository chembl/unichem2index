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
			logger.Infow(
				"WORKER RESPONSE:",
				"Succedded",
				r.Succedded,
				"Created",
				r.Created,
				"Updated",
				r.Updated,
				"Indexed",
				r.Indexed,
				"Failed",
				r.Failed,
				"Current",
				ex.CurrentCompound.UCI,
			)

			if r.BulkResponse.Errors {
				logger.Error("Bulk response reported errors")
			}
			if r.Failed > 0 {
				logger.Error("Failed records on bulk")
				ids := ""
				reasons := ""
				for _, it := range r.BulkResponse.Failed() {
					ids = ids + "," + it.Id
					reasons = reasons + " " + it.Error.Reason
				}
				logger.Error("IDs with error ", ids)
				logger.Error("Reasons: ", reasons)
			}
		}
	}()

	go func() {
		for e := range em.Errchan {
			logger.Warn("For worker started on %d Last UCI compound: %s", ex.QueryStart, ex.CurrentCompound.UCI)
			logger.Panic("Got error from bulk ", e)
		}
	}()

	err = ex.Start()
	if err != nil {
		logger.Warn("For worker started on %d Last UCI compound: %s", ex.QueryStart, ex.CurrentCompound.UCI)
		logger.Panic("Extractor error ", err)
	}
	logger.Infof("Finished worker started with UCI %d waiting for loader to finish. Last UCI compound: %s", ex.QueryStart, ex.CurrentCompound.UCI)
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
