package extractor

import (
	"context"
	"os"
	"os/signal"
	"time"

	"go.uber.org/zap"
)

var (
	logger             *zap.SugaredLogger
	config             *Configuration
	failedWorkersCount *int
)

//Init sets up exractors and loaders
func Init(l *zap.SugaredLogger, con *Configuration, eh, oraconn string) {
	logger = l
	config = con
	var err error

	em, err := getElasticManager(eh)
	if err != nil {
		logger.Error("Error create elastic manager ", err)
	}
	defer em.Close()

	logger.Info("Fetching Unichem DB")
	if config.QueryLimit <= 0 {
		logger.Panic("QueryLimit must be a number higher than 0")
	}

	ex := Extractor{
		ElasticManager: em,
		Oraconn:        oraconn,
		QueryStart:     config.QueryStart,
		QueryLimit:     config.QueryLimit,
		Logger:         logger,
	}
	ti := time.Now()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		logger.Warn("Received Interrupt Signal")
		ex.PrintLastCompound()
		elapsedTime(ti)
		os.Exit(1)
	}()

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
			)

			if r.BulkResponse.Errors {
				logger.Error("Bulk response reported errors")
			}
			if r.Failed > 0 {
				logger.Error("Failed records on bulk")
				for _, it := range r.BulkResponse.Failed() {
					logger.Error("Caused  by: ", it.Error.CausedBy)
					logger.Error("ID with error ", it.Id)
				}
				*failedWorkersCount++
			}
		}
	}()

	go func() {
		for e := range em.Errchan {
			ex.PrintLastCompound()
			logger.Panic("Got error from bulk ", e)
		}
	}()

	err = ex.Start()
	if err != nil {
		ex.PrintLastCompound()
		logger.Panic("Extractor error ", err)
	}
	logger.Infof("Finished extacting waiting for loader to finish")
	ex.PrintLastCompound()
	em.WaitGroup.Wait()
	elapsedTime(ti)
}

func getElasticManager(eh string) (*ElasticManager, error) {

	ctx := context.Background()

	if config.BulkLimit <= 0 {
		logger.Panic("BulkLimit must be a number higher than 0")
	}

	if config.MaxBulkCalls <= 0 {
		logger.Panic("MaxBulkCalls must be a number higher than 0")
	}

	es := ElasticManager{
		Context:      ctx,
		IndexName:    "unichem",
		TypeName:     "compound",
		Bulklimit:    config.BulkLimit,
		MaxBulkCalls: config.MaxBulkCalls,
	}

	err := es.Init(eh, logger)
	if err != nil {
		logger.Error("Error init ElasticManager ", err)
		return nil, err
	}

	return &es, nil

}

func elapsedTime(t time.Time) {
	e := time.Since(t)
	logger.Infof("Elapsed %s Failed: %d", e, failedWorkersCount)
}
