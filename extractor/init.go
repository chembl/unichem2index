package extractor

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"time"
	"github.com/gosuri/uiprogress"
	"go.uber.org/zap"
)

var (
	wg sync.WaitGroup
)

type extractorResponse struct {
	id          int
	queryStart  int
	lastIndexed int
}

//Init sets up exractors and loaders
func Init(l *zap.SugaredLogger, conf *Configuration) {
	ti := time.Now()
	uiprogress.Start()
	resChan := make(chan extractorResponse)
	var extractors []*Extractor
	var bars []*uiprogress.Bar

	for i, r := range conf.QueryRanges {
		if r.Finish <= 0 {
			l.Panic("QueryLimit must be a number higher than 0")
		}
		//maxUCIs := r.Finish - r.Start
		ex := Extractor{
			id:          i,
			Oraconn:     conf.OracleConn,
			Query:       conf.Query,
			QueryStart:  r.Start,
			QueryLimit:  r.Finish,
			Logger:      l,
			LastIdAdded: 0,
		}
		l.Infof("Sending extractor from %d to %d", r.Start, r.Finish)
		bar := uiprogress.AddBar(r.Finish).AppendCompleted()
		bar.PrependFunc(func(b *uiprogress.Bar) string {
			return fmt.Sprintf("Last %d", ex.LastIdAdded)
		})

		bar.AppendFunc(func(b *uiprogress.Bar) string {
			return fmt.Sprintf("%d-%d", ex.QueryStart, ex.QueryLimit)
		})
		bars = append(bars, bar)

		wg.Add(1)
		go sendExtractor(l, conf, &ex, resChan)
		extractors = append(extractors, &ex)

		time.Sleep(300 * time.Millisecond)
	}
	waitForSignal(l, ti, extractors)

	go func() {
		for res := range resChan {
			err := bars[res.id].Set(res.lastIndexed)
			if err != nil {
				panic(err)
			}
		}
	}()
	wg.Wait()
	elapsedTime(l, ti)
}

func sendExtractor(l *zap.SugaredLogger, cn *Configuration, ex *Extractor, resChan chan extractorResponse) {

	defer wg.Done()
	logger := l
	var err error

	em, err := getElasticManager(l, cn)
	if err != nil {
		logger.Panic("Error create elastic manager ", err)
		panic(err)
	}
	defer em.Close()

	ex.ElasticManager = em

	go func() {
		for r := range em.Respchan {

			if r.BulkResponse.Errors {
				logger.Error("Bulk response reported errors")
			} else {
				succeeded := r.BulkResponse.Succeeded()
				sort.Slice(succeeded, func(i, j int) bool {
					first, err := strconv.Atoi(succeeded[i].Id)
					if err != nil {
						logger.Panic("Error turning ID into int", err)
					}

					second, err := strconv.Atoi(succeeded[j].Id)
					if err != nil {
						logger.Panic("Error turning ID into int", err)
					}

					return first < second
				})

				lastSucceded := succeeded[len(succeeded)-1]
				logger.Infow(
					"WORKER_RESPONSE",
					"succeeded",
					r.Succedded,
					"indexed",
					r.Indexed,
					"failed",
					r.Failed,
					"Took",
					r.BulkResponse.Took,
					"extractorStarted",
					ex.QueryStart,
					"lastSucceeded",
					lastSucceded.Id,
				)

				li, err := strconv.Atoi(lastSucceded.Id)
				if err != nil {
					logger.Panic("Error turning ID into int")
				}
				if ex.LastIdAdded < li {
					ex.LastIdAdded = li
				}
				resChan <- extractorResponse{
					id:          ex.id,
					queryStart:  ex.QueryStart,
					lastIndexed: ex.LastIdAdded,
				}
			}
			if r.Failed > 0 {
				logger.Error("Failed records on bulk")
				ids := ""
				reasons := ""
				fr := r.BulkResponse.Failed()
				logger.Error(fr[0].Error.Reason)

				for _, it := range r.BulkResponse.Failed() {
					ids = ids + "," + it.Id
					reasons = reasons + " " + it.Error.Reason
				}

				logger.Error("IDs with error ", ids)
				logger.Debug("Reasons: ", reasons)
			}
		}
	}()

	go func() {
		for e := range em.Errchan {
			m := fmt.Sprintf("For worker started on %d Got error from bulk", ex.QueryStart)
			fmt.Println(m)
			logger.Error(m)
			logger.Panic(e)
			panic(e)
		}
	}()

	err = ex.Start()
	if err != nil {
		logger.Warnf("For worker started on %d.", ex.QueryStart)
		logger.Panic("Extractor error ", err)
		panic(err)
	}
	logger.Infof("Finished worker started with UCI %d waiting for loader to finish. Last ID: %d", ex.QueryStart, ex.LastIdAdded)
	em.WaitGroup.Wait()
}

func getElasticManager(l *zap.SugaredLogger, cn *Configuration) (*ElasticManager, error) {
	logger := l

	ctx := context.Background()

	if cn.BulkLimit <= 0 {
		logger.Panic("BulkLimit must be a number higher than 0")
	}

	if cn.MaxBulkCalls <= 0 {
		logger.Panic("MaxBulkCalls must be a number higher than 0")
	}

	es := ElasticManager{
		Context:      ctx,
		IndexName:    "unichem",
		TypeName:     "compound",
		Bulklimit:    cn.BulkLimit,
		MaxBulkCalls: cn.MaxBulkCalls,
	}

	err := es.Init(cn.ElasticHost, logger)
	if err != nil {
		logger.Error("Error init ElasticManager ", err)
		return nil, err
	}

	return &es, nil
}

func waitForSignal(l *zap.SugaredLogger, t time.Time, exs []*Extractor) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		l.Warn("Received Interrupt Signal")
		for _, ex := range exs {
			m := fmt.Sprintf("For worker started on %d Last compound UCI: %d", ex.QueryStart, ex.LastIdAdded)
			fmt.Println(m)
			l.Warn(m)
		}
		elapsedTime(l, t)
		os.Exit(1)
	}()
}

func elapsedTime(l *zap.SugaredLogger, t time.Time) {
	logger := l
	e := time.Since(t)
	m := fmt.Sprintf("Elapsed %s", e)
	fmt.Println(m)
	logger.Infof(m)
}
