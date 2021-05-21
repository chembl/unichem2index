package extractor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	elastic "github.com/olivere/elastic/v7"
	"go.uber.org/zap"
)

// Inchi splited in its components
type Inchi struct {
	Version               string `json:"version"`
	Formula               string `json:"formula"`
	ConnectionsReg        string `json:"connections"`
	HAtomsReg             string `json:"h_atoms"`
	ChargeReg             string `json:"charge"`
	ProtonsReg            string `json:"protons"`
	StereoDbondReg        string `json:"stereo_dbond"`
	StereoSP3Reg          string `json:"stereo_SP3"`
	StereoSP3invertedReg  string `json:"stereo_SP3_inverted"`
	StereoTypeReg         string `json:"stereo_type"`
	IsotopicAtoms         string `json:"isotopic_atoms"`
	IsotopicExchangeableH string `json:"isotopic_exchangeable_h"`
	Inchi                 string `json:"inchi"`
}

// CompoundSource is the source where the unichem database extracted that
// compound
type CompoundSource struct {
	ID                 int       `json:"id"`
	Name               string    `json:"name"`
	LongName           string    `json:"long_name"`
	CompoundID         string    `json:"compound_id"`
	Description        string    `json:"description"`
	BaseURL            string    `json:"base_url"`
	ShortName          string    `json:"short_name"`
	BaseIDURLAvailable bool      `json:"base_id_url_available"`
	AuxForURL          bool      `json:"aux_for_url"`
	CreatedAt          time.Time `json:"created_at"`
	LastUpdate         time.Time `json:"last_updated,omitempty"`
}

// Compound is an structure describing the information to be indexed
// extracted from Unichem database
type Compound struct {
	UCI              int              `json:"uci"`
	Inchi            Inchi            `json:"inchi"`
	StandardInchiKey string           `json:"standard_inchi_key"`
	Smiles           string           `json:"smiles"`
	Sources          []CompoundSource `json:"sources,omitempty"`
	CreatedAt        time.Time        `json:"created_at"`
	IsSourceless     bool             `json:"is_sourceless"`
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
			"number_of_shards": 10
		},
		"mappings": {
			"properties": {
				"uci": {
					"type": "integer",
					"copy_to": "known_ids"
				},
				"inchi": {
					"properties": {
						"version": { "type": "keyword" },
						"formula": { "type": "keyword" },
						"connections": { "type": "keyword" },
						"h_atoms": { "type": "keyword" },
						"charge": { "type": "keyword" },
						"protons": { "type": "keyword" },
						"stereo_dbond": { "type": "keyword" },
						"stereo_SP3": { "type": "keyword" },
						"stereo_SP3_inverted": { "type": "keyword" },
						"stereo_type": { "type": "keyword" },
						"isotopic_atoms": { "type": "keyword" },
						"isotopic_exchangeable_h": { "type": "keyword" },
						"inchi": { "type": "keyword" }
					}
				},
				"standard_inchi_key": {
					"type": "keyword"
				},
				"smiles": {
					"type": "keyword"
				},
				"sources": {
					"properties": {
						"id": {
							"type": "integer",
							"copy_to": "source_id"
						},
						"compound_id": {
							"type": "keyword",
							"copy_to": "compound_id"
						},
						"long_name": {
							"type": "keyword",
							"copy_to": "source_name"
						},
						"created_at": {
							"type": "date"
						},
						"last_updated": {
							"type": "date"
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

// AddToBulk fills a BulkRequest up to the limit set up on the em.Bulklimit property
func (em *ElasticManager) AddToBulk(c Compound) {

	em.logger.Debugw(
		"Adding to bulk: ",
		"UCI",
		c.UCI,
		"Smiles",
		c.Smiles,
		"sources",
		c.Sources,
		"isSouceless",
		c.IsSourceless)

	tmp := Compound{
		UCI:              c.UCI,
		Inchi:            c.Inchi,
		StandardInchiKey: c.StandardInchiKey,
		Sources:          c.Sources,
		Smiles:           c.Smiles,
		CreatedAt:        c.CreatedAt,
		IsSourceless:     c.IsSourceless,
	}

	if em.countBulkRequest < em.Bulklimit {
		em.countBulkRequest++
	} else {
		em.logger.Debugf("Got %d sending BulkRequest. New Bulk starting from: %d", em.countBulkRequest, c.UCI)
		if em.currentBulkCalls < em.MaxBulkCalls {
			em.currentBulkCalls++
		} else {
			// Wait for the MaxBulkCalls threads finish before continuing
			em.logger.Debugf("Hitting %d workers to send. Waiting for them to finish. Last UCI: %d", em.currentBulkCalls, c.UCI)
			em.WaitGroup.Wait()
			em.currentBulkCalls = 0
		}

		em.totalSentJobs++

		em.WaitGroup.Add(1)
		em.logger.Debugf("Sending bulk triggered by: %d", c.UCI)
		go em.sendBulkRequest(em.Context, em.Errchan, em.Respchan, em.currentBulkService, c.UCI)

		em.countBulkRequest = 1
		em.currentBulkService = em.Client.Bulk()
	}

	t := elastic.NewBulkUpdateRequest().Index(em.IndexName).DocAsUpsert(true).Id(strconv.Itoa(c.UCI)).Doc(tmp)
	em.currentBulkService = em.currentBulkService.Add(t)

}

//SendCurrentBulk through a worker, useful for cleaning the requests stored on the BulkService
//regardless the BulkLimit has been reached or not
func (em *ElasticManager) SendCurrentBulk() {
	if em.currentBulkService.NumberOfActions() > 0 {
		em.totalSentJobs++

		br, err := em.currentBulkService.Do(em.Context)
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

func (em *ElasticManager) sendBulkRequest(ctx context.Context, ce chan error, cr chan WorkerResponse, b *elastic.BulkService, UCI int) {

	defer em.WaitGroup.Done()
	em.logger.Debugf("INIT bulk worker %d", UCI)
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
	em.logger.Debugf("END bulk worker %d", UCI)
}

func (em *ElasticManager) getCount() (int64, error) {
	ctx := em.Context
	l := em.logger
	l.Info("Retrieving the total UCI count")

	countResult, err := em.Client.Count().Index(em.IndexName).Do(ctx)
	if err != nil {
		m := fmt.Sprint("Error getting getting last updated UCI", err)
		fmt.Println(m)
		l.Fatal(m)
		return 0, err
	}
	l.Info("Elastic count result: ", countResult)

	return countResult, err
}

func (em *ElasticManager) getLastIndexedUCI() (int, error) {
	ctx := em.Context
	l := em.logger
	l.Info("Retrieving last UCI indexed")
	termQuery := elastic.NewMatchAllQuery()
	searchResults, err := em.Client.Search().Index(em.IndexName).Query(termQuery).Sort("uci", false).Size(1).Do(ctx)
	if err != nil {
		m := fmt.Sprint("Error getting getting last UCI indexed", err)
		fmt.Println(m)
		l.Fatal(m)

		return 0, err
	}
	var c Compound
	if searchResults.Hits.TotalHits.Value > 0 {
		for _, hit := range searchResults.Hits.Hits {
			err := json.Unmarshal(hit.Source, &c)
			if err != nil {
				m := fmt.Sprint("Error deserialize", err)
				fmt.Println(m)
				l.Fatal(m)
				return 0, err
			}
			return c.UCI, nil
		}
	} else {
		return 0, errors.New("no last UCI found")
	}

	return 0, err
}

func (em *ElasticManager) getLastUpdated() (time.Time, error) {
	ctx := em.Context
	l := em.logger
	l.Info("Retrieving the last updated UCI")

	termQuery := elastic.NewMatchAllQuery()
	aggLstUp := elastic.NewMaxAggregation().Field("sources.last_updated")
	aggCtAt := elastic.NewMaxAggregation().Field("sources.created_at")
	searchResults, err := em.Client.Search().Index(em.IndexName).Query(termQuery).Aggregation("max_last_updated", aggLstUp).Aggregation("max_created", aggCtAt).Do(ctx)
	if err != nil {
		m := fmt.Sprint("Error getting getting last updated UCI", err)
		fmt.Println(m)
		l.Fatal(m)

		return time.Now(), err
	}

	maxLastUpdated, found := searchResults.Aggregations.MaxBucket("max_last_updated")
	if !found {
		m := fmt.Sprint("max_last_updated aggregation not found", err)
		fmt.Println(m)
		l.Fatal(m)

		return time.Now(), err
	}
	l.Debug("Max last updated", maxLastUpdated.ValueAsString)
	tm := int64(*maxLastUpdated.Value) / 1000
	mu := time.Unix(tm, 0)

	maxCreated, found := searchResults.Aggregations.MaxBucket("max_created")
	if !found {
		m := fmt.Sprint("max_created aggregation not found", err)
		fmt.Println(m)
		l.Fatal(m)

		return time.Now(), err
	}
	l.Debug("Max Created", maxCreated.ValueAsString)
	tm = int64(*maxCreated.Value) / 1000
	mc := time.Unix(tm, 0)
	oldest := mu
	if mc.Before(mu) {
		oldest = mc
	}

	m := fmt.Sprint("Oldest date: ", oldest)
	fmt.Println(m)
	l.Info(m)

	return oldest, err
}

//Close terminates the ElasticSearch Client and BulkProcessor
func (em *ElasticManager) Close() {
	em.Client.Stop()
}
