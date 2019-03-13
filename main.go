package main

import (
	"path/filepath"
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
	failedWorkersCount *int
	config  *extractor.Configuration
)

func logInit(d bool, logPath string) *os.File {

	path := filepath.Join(logPath, "unichem2index.log")
	fmt.Println("Path to log ", path)
	// Open file for writing
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}

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
		zapcore.NewCore(fileEncoder, zapcore.AddSync(file), level),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
	)

	l := zap.New(core)

	logger = l.Sugar()

	return file
}

func getElasticManager(eh string) (*loader.ElasticManager, error) {

	ctx := context.Background()

	if config.BulkLimit <= 0 {
		logger.Panic("BulkLimit must be a number higher than 0")
	}

	if config.MaxBulkCalls <= 0 {
		logger.Panic("MaxBulkCalls must be a number higher than 0")
	}

	es := loader.ElasticManager{
		Context:   ctx,
		IndexName: "unichem",
		TypeName:  "compound",
		Bulklimit: config.BulkLimit,
		MaxBulkCalls: config.MaxBulkCalls,
	}

	err := es.Init(eh, logger)
	if err != nil {
		logger.Error("Error init ElasticManager ", err)
		return nil, err
	}

	return &es, nil

}

func extractUnichemData(oraconn string, em *loader.ElasticManager) {
	logger.Info("Fetching Unichem DB")
	if config.QueryLimit <= 0 {
		logger.Panic("QueryLimit must be a number higher than 0")
	}	
	err := extractor.StartExtraction(em, logger, oraconn, config.QueryLimit, config.QueryStart)
	if err != nil {
		logger.Error("Extractor error ", err)
	}
}

func main() {

	d := flag.Bool("d", false, "Sets up the log level to debug, keep in mind logging will have an impact on the performance")
	v := flag.Bool("v", false, "Returns the binary version and built date info")
	eh := flag.String("eshost", "", "ElasticSearch host, Example: http://0.0.0.0:9200")
	oraconn := flag.String("oraconn", "", "Oracle Database connection string: Example: 'hr/hr@localhost:1521:XE'")
	flag.Parse()
	
	var err error

	config, err = extractor.LoadConfig()
	if err != nil {
		panic("Couldn't load config.yml file")
	}
 
	f := logInit(*d, config.LogPath)
	defer f.Close()

	ti := time.Now()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(){
		<-c
		logger.Warn("Received Interrupt Signal")
		timeElapsed(ti)
		os.Exit(1)
	}()

	logger.Info("--------------Init program--------------")
	logger.Info(fmt.Sprintf("Version: %s Build Date: %s", version, buildDate))
	logger.Infow(
		"Configuration",
		"ES index",
		config.Index,
		"ES type",
		config.Type,
		"Query start",
		config.QueryStart,
		"Query limit",
		config.QueryLimit,
		"Bulk limit",
		config.BulkLimit,
		"Maximum Bulk calls",
		config.MaxBulkCalls,
		)

	if len(*eh) <= 0 {
		eh = &config.ElasticHost
		if len(*eh) <= 0 {
			logger.Panic("Please provide the ElasticSearch host")
		}
	}
	logger.Info("Elastic host ", *eh)

	if len(*oraconn) <= 0 {
		oraconn = &config.OracleConn
		if len(*oraconn) <= 0 {
			logger.Panic("Please provide a Oracle Connection string")
		} 
	}
	logger.Info("Oracle connection string ", *oraconn)

	if *v {
		return
	}

	em, err := getElasticManager(*eh)
	if err != nil {
		logger.Error("Error create elastic manager ", err)
	}
	defer em.Close()

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
	logger.Infof("Elapsed %s Failed: %d", e, *failedWorkersCount)
}