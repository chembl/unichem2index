package extractor

import (
	"context"
	"sync"
	"time"

	"github.com/olivere/elastic"
	"go.uber.org/zap"
)

// CompoundSource is the source where the unichem database extracted that
// compound
type CompoundSource struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	LongName    string `json:"long_name"`
	CompoundID  string `json:"compound_id"`
	Description string `json:"description"`
	BaseURL     string `json:"base_url"`
}

// Compound is an structure describing the information to be indexed
// extracted from Unichem database
type Compound struct {
	UCI              string           `json:"uci,omitempty"`
	Inchi            string           `json:"inchi"`
	StandardInchiKey string           `json:"standard_inchi_key"`
	Smiles           string           `json:"smiles"`
	Sources          []CompoundSource `json:"sources,omitempty"`
	CreatedAt        time.Time        `json:"created_at"`
}

// WorkerResponse contains the result of the BulkRequest to the ElasticSearch index
type WorkerResponse struct {
	Succedded    int
	Indexed      int
	Created      int
	Updated      int
	Deleted      int
	Failed       int
	BulkResponse *elastic.BulkResponse
}

// ElasticManager used for connection and adding compounds to the
// elastic server
type ElasticManager struct {
	logger             *zap.SugaredLogger
	Context            context.Context
	Client             *elastic.Client
	IndexName          string
	TypeName           string
	Bulklimit          int
	countBulkRequest   int
	currentBulkService *elastic.BulkService
	Errchan            chan error
	Respchan           chan WorkerResponse
	WaitGroup          sync.WaitGroup
	currentBulkCalls   int
	MaxBulkCalls       int
	totalSentJobs      int
	totalDoneJobs      int
}

// Init function initializes an elastic client and pings it to check the provider server is up
func (em *ElasticManager) Init(ctx context.Context, conf *Configuration, logger *zap.SugaredLogger) error {
	em.logger = logger
	// ctx = context.Background()
	em.Context = ctx

	var err error

	mapping := `{
		"settings": {
			"refresh_interval": -1,
			"number_of_replicas": 1,
			"number_of_shards": 5
		},
		"mappings": {
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
				"smiles": {
					"type": "keyword",
					"fields": {
						"similarity": {
							"type": "similarity_fingerprint"
						},
						"substructure": {
							"type": "structure_fingerprint"
						}
					}
				},
				"sources": {
					"type": "nested",
					"properties": {
						"id": {
							"type": "keyword",
							"copy_to": "source_id"
						},
						"compound_id": {
							"type": "keyword",
							"copy_to": "compound_id"
						},
						"long_name": {
							"type": "keyword",
							"copy_to": "source_name"
						}
					}
				}
			}
		}
	}`

	em.Client, err = elastic.NewClient(
		elastic.SetURL(conf.ElasticHost),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(conf.ElasticAuth.Username, conf.ElasticAuth.Password),
	)
	if err != nil {
		em.logger.Panic("Error connecting to ElasticSearch ", err)
		return err
	}

	inf, code, err := em.Client.Ping(conf.ElasticHost).Do(ctx)
	if err != nil {
		em.logger.Panic("Error Pinging elastic client ", err)
		return err
	}
	em.logger.Infof("Succesfully pinged ElasticSearch server with code %d and version %s", code, inf.Version.Number)

	ex, err := em.Client.IndexExists(em.IndexName).Do(ctx)
	if err != nil {
		em.logger.Panic("Error fetchin index existence ", err)
		return err
	}

	if !ex {
		in, err := em.Client.CreateIndex(em.IndexName).BodyString(mapping).Do(ctx)
		em.logger.Infof("Creating index %s", em.IndexName)
		if err != nil {
			em.logger.Panic("Error creating index  ", err)
			return err
		}

		if !in.Acknowledged {
			em.logger.Error("Index creation not acknowledged")
			return err
		}
		// Giving ES time to set up the Index
		time.Sleep(2 * time.Second)
	} else {
		em.logger.Infof("Index %s exist, skipping its creation", em.IndexName)
	}

	em.currentBulkService = em.Client.Bulk()
	em.currentBulkCalls = 0
	em.countBulkRequest = 0
	em.totalSentJobs = 0
	em.totalDoneJobs = 0

	em.Errchan = make(chan error)
	em.Respchan = make(chan WorkerResponse)
	return nil
}

// AddToIndex fills a BulkRequest up to the limit set up on the em.Bulklimit property
func (em *ElasticManager) AddToIndex(c Compound) {

	ctx := em.Context

	em.logger.Debugw(
		"Adding to index: ",
		"UCI",
		c.UCI,
		"Smiles",
		c.Smiles,
		"sources",
		c.Sources)

	tmp := Compound{
		Inchi:            c.Inchi,
		StandardInchiKey: c.StandardInchiKey,
		Sources:          c.Sources,
		Smiles:           c.Smiles,
		CreatedAt:        c.CreatedAt,
	}

	if em.countBulkRequest < em.Bulklimit {
		em.countBulkRequest++
	} else {
		em.logger.Debugf("Got %d sending BulkRequest. New Bulk starting from: %s", em.countBulkRequest, c.UCI)
		if em.currentBulkCalls < em.MaxBulkCalls {
			em.currentBulkCalls++
		} else {
			// Wait for the MaxBulkCalls threads finish before continuing
			em.logger.Debugf("Hitting %d workers to send. Waiting for them to finish. Last UCI: %s", em.currentBulkCalls, c.UCI)
			em.WaitGroup.Wait()
			em.currentBulkCalls = 0
		}

		em.totalSentJobs++
		em.WaitGroup.Add(1)
		go em.sendBulkRequest(ctx, em.Errchan, em.Respchan, em.currentBulkService)

		em.countBulkRequest = 1
		em.currentBulkService = em.Client.Bulk()
	}

	t := elastic.NewBulkIndexRequest().Index(em.IndexName).Id(c.UCI).Doc(tmp)
	em.currentBulkService = em.currentBulkService.Add(t)

}

//SendCurrentBulk throught a worker, useful for cleaning the requests stored on the BulkService
//regardless the BulkLimit has been reached or not
func (em *ElasticManager) SendCurrentBulk() {
	ctx := em.Context
	if em.currentBulkService.NumberOfActions() > 0 {
		em.totalSentJobs++

		br, err := em.currentBulkService.Do(ctx)
		if err != nil {
			em.Errchan <- err
			return
		}
		wr := WorkerResponse{
			Succedded:    len(br.Succeeded()),
			Indexed:      len(br.Indexed()),
			Created:      len(br.Created()),
			Updated:      len(br.Updated()),
			Failed:       len(br.Failed()),
			BulkResponse: br,
		}

		em.Respchan <- wr
		em.logger.Debug("END last bulk sent")
	} else {
		em.logger.Warn("No actions on current bulk service, skipping last bulk")
	}
}

func (em *ElasticManager) sendBulkRequest(ctx context.Context, ce chan error, cr chan WorkerResponse, b *elastic.BulkService) {

	defer em.WaitGroup.Done()
	em.logger.Debugf("INIT bulk worker")
	br, err := b.Do(ctx)
	if err != nil {
		ce <- err
		return
	}
	wr := WorkerResponse{
		Succedded:    len(br.Succeeded()),
		Indexed:      len(br.Indexed()),
		Created:      len(br.Created()),
		Updated:      len(br.Updated()),
		Failed:       len(br.Failed()),
		BulkResponse: br,
	}
	cr <- wr
	em.logger.Debugf("END bulk worker ")
}

//Close terminates the ElasticSearch Client and BulkProcessor
func (em *ElasticManager) Close() {
	em.Client.Stop()
}
