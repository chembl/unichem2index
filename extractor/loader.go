package extractor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"
	"go.uber.org/zap"
)

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
	AuxSrc             string    `json:"aux_src"`
	AuxForURL          bool      `json:"aux_for_url"`
	CreatedAt          time.Time `json:"created_at"`
	LastUpdate         time.Time `json:"last_updated,omitempty"`
	IsPrivate          bool      `json:"is_private"`
}

// Compound is an structure describing the information to be indexed
// extracted from Unichem database
type Compound struct {
	UCI              int              `json:"uci"`
	Inchi            Inchi            `json:"inchi"`
	Components       []Inchi          `json:"components,omitempty"`
	StandardInchiKey string           `json:"standard_inchi_key"`
	Smiles           string           `json:"smiles"`
	Sources          []CompoundSource `json:"sources,omitempty"`
	CreatedAt        time.Time        `json:"created_at"`
	IsSourceless     bool             `json:"is_sourceless"`
}

// UCICount is the amount of UCI by Sources on UniChem
type UCICount struct {
	TotalUCI int `json:"totalUCI"`
	Source   int `json:"source"`
}

// WorkerResponse contains the result of the BulkRequest to the ElasticSearch index
type WorkerResponse struct {
	Succeeded    int
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

	if len(conf.ESIndexSettings) <= 0 {
		logger.Panic("ES Index Setting can't be empty. PLease provide a valid one on the configuration file")
	}

	mapping := conf.ESIndexSettings

	var err error
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
		em.logger.Panic("Error fetching index existence ", err)
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

	t := elastic.NewBulkUpdateRequest().Index(em.IndexName).DocAsUpsert(true).Id(strconv.Itoa(c.UCI)).Doc(c)
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
			Succeeded:    len(br.Succeeded()),
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
		Succeeded:    len(br.Succeeded()),
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
		m := fmt.Sprint("Error getting index total UCI", err)
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

func (em *ElasticManager) getUCICountBySources() (map[int]UCICount, error) {
	ctx := em.Context
	l := em.logger
	l.Info("Retrieving UCI count by sources")

	aggUCISou := elastic.NewTermsAggregation().Field("sources.id").Size(3000).OrderByCountDesc()
	searchResults, err := em.Client.Search().Index(em.IndexName).Size(0).Aggregation("uci_by_sources_count", aggUCISou).Do(ctx)
	if err != nil {
		m := fmt.Sprint("Error getting getting last updated UCI", err)
		fmt.Println(m)
		l.Fatal(m)

		return nil, err
	}

	uciCountAgg, found := searchResults.Aggregations.Terms("uci_by_sources_count")
	if !found {
		m := fmt.Sprint("uci_by_sources_count aggregation not found", err)
		fmt.Println(m)
		l.Fatal(m)

		return nil, err
	}

	uca := make(map[int]UCICount)
	for _, bu := range uciCountAgg.Buckets {
		sid := bu.KeyNumber
		s, err := sid.Int64()
		if err != nil {
			l.Error("Failed to parse the SourceID from the bucket")
			return nil, err
		}
		uca[int(s)] = UCICount{
			TotalUCI: int(bu.DocCount),
			Source:   int(s),
		}
	}
	l.Debug("Map UCI count:", uca)
	return uca, err
}

//Close terminates the ElasticSearch Client and BulkProcessor
func (em *ElasticManager) Close() {
	em.Client.Stop()
}
