package extractor

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

//Configuration stores the configuration parameters required for the application
type Configuration struct {
	LogPath      string
	OracleConn   string
	ElasticHost  string
	QueryStart   int
	QueryLimit   int
	BulkLimit    int
	Index        string
	Type         string
	MaxBulkCalls int
}

//LoadConfig opening a yaml config file (config.yaml)
func LoadConfig() (*Configuration, error) {

	var t Configuration

	data, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		return &t, err
	}

	err = yaml.Unmarshal(data, &t)
	if err != nil {
		return &t, err
	}

	return &t, nil
}
