package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chembl/unichem2index/extractor"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	//Driver for Oracle database
	_ "github.com/godror/godror"
)

var (
	version   string
	buildDate string
	logger    *zap.SugaredLogger
	config    *extractor.Configuration
)

func logInit(d bool, logPath string) *os.File {
	// now := time.Now()
	// fn := fmt.Sprintf("unichem2index_%s.log", now.Format("20060102_150405"))
	fn := "unichem2index.log"
	path := filepath.Join(logPath, fn)
	fmt.Println("Log path ", path)
	// Open file for writing
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}

	pe := zap.NewProductionEncoderConfig()
	pe.EncodeTime = zapcore.ISO8601TimeEncoder

	fileEncoder := zapcore.NewJSONEncoder(pe)

	level := zap.InfoLevel
	if d {
		level = zap.DebugLevel
	}

	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, zapcore.AddSync(file), level),
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

	greeting()

	if len(*eh) > 0 {
		config.ElasticHost = *eh
	} else if len(config.ElasticHost) <= 0 {
		m := "Please provide an ElasticSearch host"
		logger.Panic(m)
		panic(m)
	}
	m := fmt.Sprintf("Elastic host %s", config.ElasticHost)
	logger.Info(m)
	fmt.Println(m)

	if len(*oraconn) > 0 {
		config.OracleConn = *oraconn
	} else if len(config.OracleConn) <= 0 {
		m := "Please provide an Oracle Connection string"
		logger.Panic(m)
		panic(m)
	}
	m = fmt.Sprintf("Oracle connection string %s", config.OracleConn)
	logger.Info(m)
	fmt.Println(m)

	if *v {
		return
	}

	extractor.Init(logger, config)
}

func greeting() {
	logger.Info("--------------Init program--------------")
	logger.Info(fmt.Sprintf("Version: %s Build Date: %s", version, buildDate))
	logger.Infow(
		"Configuration",
		"ES index",
		config.Index,
		"ES type",
		config.Type,
		"Query range",
		config.QueryMax,
		"Bulk limit",
		config.BulkLimit,
		"Maximum Bulk calls",
		config.MaxBulkCalls,
	)

	fmt.Println("--------------Init program--------------")
	fmt.Printf("Version: %s Build Date: %s \n", version, buildDate)
	fmt.Printf("From %d to %d \n", config.QueryMax.Start, config.QueryMax.Finish)

}
