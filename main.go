package main

import (
	"os/signal"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/chembl/unichem2index/extractor"
	"github.com/chembl/unichem2index/loader"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	version    string
	buildDate  string
	logger     *zap.SugaredLogger
	queryLimit = 100000
	bulklimit = 1000
	maxBulkCalls = 10
)

func logInit(d bool, f *os.File) *zap.SugaredLogger {

	pe := zap.NewProductionEncoderConfig()

	fileEncoder := zapcore.NewJSONEncoder(pe)

	pe.EncodeTime = zapcore.ISO8601TimeEncoder
	pe.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleEncoder := zapcore.NewConsoleEncoder(pe)

	level := zap.InfoLevel
	if d {
		level = zap.DebugLevel
	}

	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, zapcore.AddSync(f), level),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
	)

	l := zap.New(core)

	return l.Sugar()
}

func getElasticManager(eh string) (*loader.ElasticManager, error) {

	ctx := context.Background()

	es := loader.ElasticManager{
		Context:   ctx,
		IndexName: "unichem",
		TypeName:  "compound",
		Bulklimit: bulklimit,
		MaxBulkCalls: maxBulkCalls,
	}

	err := es.Init(eh, logger)
	if err != nil {
		logger.Error("Error init ElasticManager ", err)
		return nil, err
	}

	return &es, nil

}

func initElasticManager(eh string) *loader.ElasticManager {

	logger.Info("Using Elastic search host ", eh)

	elasticManager, err := getElasticManager(eh)
	if err != nil {
		logger.Error("Error create elastic manager ", err)
	}

	logger.Info("Elastic search init successfully")
	return elasticManager
}

func extractUnichemData(oraconn string, em *loader.ElasticManager) {
	logger.Info("Fetching Unichem DB")
	err := extractor.StartExtraction(em, logger, oraconn, queryLimit)
	if err != nil {
		logger.Error("Extractor error ", err)
	}
}

func main() {
	d := flag.Bool("d", false, "Sets up the log level to debug, keep in mind logging will have an impact on the performance")
	v := flag.Bool("v", false, "Returns the binary version and built date info")
	eh := flag.String("eshost", "http://0.0.0.0:9200", "ElasticSearch host, default http://0.0.0.0:9200")
	oraconn := flag.String("oraconn", "hr/hr@localhost:1521:XE", "Oracle Database connection string: Default 'hr/hr@localhost:1521:XE'")
	flag.Parse()

	logPath := "./unichem2index.log"

	// Open file for writing
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	logger = logInit(*d, file)
	logger.Info("--------------Init program--------------")
	logger.Info(fmt.Sprintf("Version: %s Build Date: %s", version, buildDate))
	
	ti := time.Now()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(){
		<-c
		logger.Warn("Received Interrupt Signal")
		timeElapsed(ti)
		os.Exit(1)
	}()


	if *v {
		return
	}

	em := initElasticManager(*eh)
	defer em.Close()
	

	go func() {
		for r := range em.Respchan {
			if r.IsSuccesful {
				logger.Debugw(
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
			} else {
				logger.Errorw(
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
			}

		}
	}()

	go func() {
		for e := range em.Errchan {
			logger.Panic("Got error from bulk ", e)
		}
	}()

	extractUnichemData(*oraconn, em)
	logger.Infof("Finished extacting and loading")
	
	em.WaitGroup.Wait()
	timeElapsed(ti)
}

func timeElapsed(t time.Time) {
	e :=  time.Since(t)
	logger.Infof("Elapsed %s", e)
}