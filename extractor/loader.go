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
	ID   string `json:"compound_id"`
	Name string `json:"source_name"`
}

// Compound is an structure describing the information to be indexed
// extracted from Unichem database
type Compound struct {
	UCI              string           `json:"uci,omitempty"`
	Inchi            string           `json:"inchi"`
	StandardInchiKey string           `json:"standard_inchi_key"`
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
}

// Init function initializes an elastic client and pings it to check the provider server is up
func (em *ElasticManager) Init(host string, logger *zap.SugaredLogger) error {
	em.logger = logger

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
	if err != nil {
		em.logger.Panic("Error connecting to ElasticSearch ", err)
		return err
	}

	inf, code, err := em.Client.Ping(host).Do(em.Context)
	if err != nil {
		em.logger.Panic("Error Pinging elastic client ", err)
		return err
	}
	em.logger.Infof("Succesfully pinged ElasticSearch server with code %d and version %s", code, inf.Version.Number)

	ex, err := em.Client.IndexExists(em.IndexName).Do(em.Context)
	if err != nil {
		em.logger.Panic("Error fetchin index existence ", err)
		return err
	}

	if !ex {
		in, err := em.Client.CreateIndex(em.IndexName).BodyString(mapping).Do(em.Context)
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
		time.Sleep(1 * time.Second)
	} else {
		em.logger.Infof("Index %s exist, skipping its creation", em.IndexName)
	}

	em.currentBulkService = em.Client.Bulk()
	em.currentBulkCalls = 0
	em.countBulkRequest = 0

	em.Errchan = make(chan error)
	em.Respchan = make(chan WorkerResponse)

	return nil
}

// AddToIndex fills a BulkRequest up to the limit setted up on the em.Bulklimit property
func (em *ElasticManager) AddToIndex(c Compound) {
	em.logger.Debugw("Adding to index: ", "UCI", c.UCI, "sources", c.Sources)

	tmp := Compound{
		Inchi:            c.Inchi,
		StandardInchiKey: c.StandardInchiKey,
		Sources:          c.Sources,
		CreatedAt:        c.CreatedAt,
	}

	if em.countBulkRequest < em.Bulklimit {
		em.countBulkRequest++
	} else {
		em.logger.Infof("Got %d sending BulkRequest", em.countBulkRequest)
		if em.currentBulkCalls < em.MaxBulkCalls {
			em.currentBulkCalls++
		} else {
			// Wait for the MaxBulkCalls threads finish before continuing
			em.logger.Infof("Hitting %d workers to send. Waiting for %d to finish", em.currentBulkCalls, em.MaxBulkCalls)
			em.WaitGroup.Wait()
			em.currentBulkCalls = 0
		}

		em.WaitGroup.Add(1)
		go em.sendBulkRequest(em.Context, em.Errchan, em.Respchan, *em.currentBulkService)

		em.countBulkRequest = 1
		em.currentBulkService = em.Client.Bulk()
	}

	t := elastic.NewBulkIndexRequest().Index(em.IndexName).Type(em.TypeName).Id(c.UCI).Doc(tmp)
	em.currentBulkService = em.currentBulkService.Add(t)

}

//SendCurrentBulk throught a worker, useful for cleaning the requests stored on the BulkService
//regardles the BulkLimit has been reached or not
func (em *ElasticManager) SendCurrentBulk() {
	em.WaitGroup.Add(1)
	em.logger.Warn("Sending last bulk")
	go em.sendBulkRequest(em.Context, em.Errchan, em.Respchan, *em.currentBulkService)
}

func (em *ElasticManager) sendBulkRequest(ctx context.Context, ce chan error, cr chan WorkerResponse, cb elastic.BulkService) {
	defer em.WaitGroup.Done()
	em.logger.Debug("INIT bulk worker: Started")
	br, err := cb.Do(ctx)
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
	em.logger.Debug("END bulk worker")
}

//Close terminates the ElasticSearch Client and BulkProcessor
func (em *ElasticManager) Close() {
	em.Client.Stop()
}