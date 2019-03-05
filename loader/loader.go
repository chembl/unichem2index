package loader

import (
	"context"
	"fmt"
	"time"

	"github.com/olivere/elastic"
	"go.uber.org/zap"
)

// CompoundSource is the source where the unichem database extracted that
// compound
type CompoundSource struct {
	ID   string `json:"compound_id"`
	Name string `json:"source_name"`
}

// Compound is an structure describing the information to be indexed
// extracted from Unichem database
type Compound struct {
	UCI              string           `json:"uci,omitempty"`
	Inchi            string           `json:"inchi"`
	StandardInchiKey string           `json:"standard_inchi_key"`
	Sources          []CompoundSource `json:"sources"`
	CreatedAt        time.Time        `json:"created_at"`
}

// ElasticManager used for connection and adding compounds to the
// elastic server
type ElasticManager struct {
	Context       context.Context
	Client        *elastic.Client
	BulkProcessor *elastic.BulkProcessor
	IndexName     string
	TypeName      string
}

// Init function initializes an elastic client and pings it to check the provider server is up
func (em *ElasticManager) Init(host string, logger *zap.SugaredLogger) error {
	var err error

	mapping := `{
		"mappings": {
			"compound": {
				"properties": {
					"uci": {
						"type": "keyword",
						"copy_to": "known_ids"
					},
					"inchi": {
						"type": "keyword"
					},
					"standard_inchi_key": {
						"type": "keyword"
					},
					"sources": {
						"type": "nested",
						"properties": {
							"compound_id": {
								"type": "keyword",
								"copy_to": "known_ids"
							},
							"source_name": {
								"type": "keyword"
							}
						}
					}
				}
			}
		}
	}`

	em.Client, err = elastic.NewClient(
		elastic.SetURL(host),
		elastic.SetSniff(false),
	)

	inf, code, err := em.Client.Ping(host).Do(em.Context)
	if err != nil {
		logger.Panic("Error Pinging elastic client ", err)
		return err
	}
	logger.Info(fmt.Sprintf("Elasticsearch returned with code %d and version %s\n", code, inf.Version.Number))

	ex, err := em.Client.IndexExists(em.IndexName).Do(em.Context)
	if err != nil {
		logger.Panic("Error fetchin index existence ", err)
		return err
	}

	if !ex {
		in, err := em.Client.CreateIndex(em.IndexName).BodyString(mapping).Do(em.Context)
		logger.Infof("Creating index %s", em.IndexName)
		if err != nil {
			logger.Panic("Error creating index  ", err)
			return err
		}

		if !in.Acknowledged {
			logger.Error("Index creation not acknowledged")
			return err
		}
	} else {
		logger.Infof("Index %s found", em.IndexName)
	}

	em.BulkProcessor, err = em.Client.BulkProcessor().
		BulkActions(1000).
		Workers(4).
		FlushInterval(10 * time.Second).
		Stats(true).
		Do(em.Context)
	if err != nil {
		logger.Panic("FATAL trying to create bulk processor service ", err)
	}

	logger.Info("Elastic search init successfully")

	return nil
}

// SendToElastic adds a compound instance into the Index
func (em *ElasticManager) SendToElastic(c Compound, logger *zap.SugaredLogger) error {
	logger.Debugw("Adding to index: ", "UCI", c.UCI, "sources", c.Sources)

	tmp := Compound{
		Inchi:            c.Inchi,
		StandardInchiKey: c.StandardInchiKey,
		Sources:          c.Sources,
		CreatedAt:        c.CreatedAt,
	}

	r, err := em.Client.Index().Index(em.IndexName).Type(em.TypeName).Id(c.UCI).BodyJson(tmp).Do(em.Context)
	if err != nil {
		logger.Panic("Error saving UCI", err)
		return err
	}
	logger.Debugf("Added compound UCI <%s>", c.UCI)

	if r.Result == "updated" {
		logger.Warn("ID UPDATED ", c.UCI)
	}

	return nil
}

//Close terminates the ElasticSearch Client and BulkProcessor
func (em *ElasticManager) Close(logger *zap.SugaredLogger) error {
	err := em.BulkProcessor.Close()
	if err != nil {

		return err
	}

	em.Client.Stop()

	return nil
}
