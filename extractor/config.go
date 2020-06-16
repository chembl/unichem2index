package extractor

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

//Range UCI for concurrent queries
type Range struct {
	Start, Finish int
}

//Range UCI for concurrent queries
type ElasticAuth struct {
	Username, Password string
}

//Configuration stores the configuration parameters required for the application
type Configuration struct {
	LogPath       string
	OracleConn    string
	ElasticHost   string
	BulkLimit     int
	Index         string
	Type          string
	MaxBulkCalls  int
	QueryMax      Range
	Query         string
	MaxConcurrent int
	Interval      int
	MaxAttempts   int
	ElasticAuth   ElasticAuth
}

//LoadConfig opening a yaml config file (config.yaml)
func LoadConfig(c string) (*Configuration, error) {

	var t Configuration
	var fn string

	if len(c) > 0 {
		fn = c
	} else {
		fn = "config.yaml"
	}

	fmt.Printf("Using config path: %s \n", fn)

	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return &t, err
	}

	err = yaml.Unmarshal(data, &t)
	if err != nil {
		return &t, err
	}

	return &t, nil
}
