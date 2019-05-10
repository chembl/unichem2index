package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/chembl/unichem2index/extractor"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	//Driver for Oracle database
	_ "gopkg.in/goracle.v2"
)

var (
	version   string
	buildDate string
	logger    *zap.SugaredLogger
	config    *extractor.Configuration
)

func logInit(d bool, logPath string) *os.File {
	now := time.Now()
	fn := fmt.Sprintf("unichem2index_%s.log", now.Format("20060102_150405"))
	path := filepath.Join(logPath, fn)
	fmt.Println("Log path ", path)
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

func main() {

	cn := flag.String("config", "", "Config file path, must be YAML")
	d := flag.Bool("d", false, "Sets up the log level to debug, keep in mind logging will have an impact on the performance")
	v := flag.Bool("v", false, "Returns the binary version and built date info")
	eh := flag.String("eshost", "", "ElasticSearch host, Example: http://0.0.0.0:9200")
	oraconn := flag.String("oraconn", "", "Oracle Database connection string: Example: 'hr/hr@localhost:1521:XE'")
	flag.Parse()

	var err error

	config, err = extractor.LoadConfig(*cn)
	if err != nil {
		fmt.Println(err)
		panic("Couldn't load config.yml file")
	}

	f := logInit(*d, config.LogPath)
	defer f.Close()

	logger.Info("--------------Init program--------------")
	logger.Info(fmt.Sprintf("Version: %s Build Date: %s", version, buildDate))
	logger.Infow(
		"Configuration",
		"ES index",
		config.Index,
		"ES type",
		config.Type,
		"Query range",
		config.QueryRanges,
		"Bulk limit",
		config.BulkLimit,
		"Maximum Bulk calls",
		config.MaxBulkCalls,
	)

	if len(*eh) > 0 {
		config.ElasticHost = *eh
	} else if len(config.ElasticHost) <= 0 {
		logger.Panic("Please provide an ElasticSearch host")
	}
	logger.Info("Elastic host ", config.ElasticHost)

	if len(*oraconn) > 0 {
		config.OracleConn = *oraconn
	} else if len(config.OracleConn) <= 0 {
		logger.Panic("Please provide an Oracle Connection string")
	}
	logger.Info("Oracle connection string ", config.OracleConn)

	if *v {
		return
	}

	extractor.Init(logger, config)

	logger.Info("End of process")
}
