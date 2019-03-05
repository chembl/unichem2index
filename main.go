package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/chembl/unichem-to-index/extractor"
	"github.com/chembl/unichem-to-index/loader"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	version        string
	buildDate      string
	logger *zap.SugaredLogger
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

func getElasticManager(eh string) (loader.ElasticManager, error) {

	ctx := context.Background()

	es := loader.ElasticManager{
		Context:   ctx,
		IndexName: "unichem",
		TypeName:  "compound",
	}

	err := es.Init(eh, logger)
	if err != nil {
		logger.Error("Error init ElasticManager ", err)
		return es, err
	}

	return es, nil

}

func initElasticManager(eh string) *loader.ElasticManager {

	logger.Info("Using Elastic search host ", eh)

	elasticManager, err := getElasticManager(eh)
	if err != nil {
		logger.Error("Error create elastic manager ", err)
	}

	return &elasticManager
}

func extractUnichemData(oraconn string, em *loader.ElasticManager) {

	err := extractor.StartExtraction(em, logger, oraconn)
	if err != nil {
		logger.Error("Extractor error ", err)
	}
}

func main() {
	d := flag.Bool("d", false, "Sets up the log level to debug")
	v := flag.Bool("v", false, "Returns the binary version and built date info")
	eh := flag.String("eshost", "http://0.0.0.0:9200", "ElasticSearch host, default http://0.0.0.0:9200")
	oraconn := flag.String("oraconn", "hr/hr@localhost:1521:XE", "Oracle Database connection string: Default 'hr/hr@localhost:1521:XE'")
	flag.Parse()

	logPath := "./unichem2index.log"

    // Open file for writing
    file, err := os.OpenFile(logPath, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0777)
    if err != nil {
       panic(err)
    }
    defer file.Close()

	logger = logInit(*d, file)
	logger.Info("--------------Init program--------------")
	logger.Info(fmt.Sprintf("Version: %s Build Date: %s", version, buildDate))

	if *v {
		return
	}

	em := initElasticManager(*eh)
	defer em.Close(logger)

	extractUnichemData(*oraconn, em)

}