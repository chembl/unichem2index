package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"time"
)

type Elnesteado struct {
	Id          string `json:"id,omitempty"`
	Description string `json:"description,omitempty"`
}

type Testy struct {
	Name      string       `json:"name,omitempty"`
	Nesteados []Elnesteado `json:"el_nesteado,omitempty"`
}

func main() {
	getLastUpdated()
}

func getLastUpdated(){
	ctx := context.Background()
	indexName := "unichem"
	host := "http://hx-rke-wp-webadmin-04-worker-2.caas.ebi.ac.uk:32000"
	fmt.Println("COnnecting to ES")
	client, err := elastic.NewClient(
		elastic.SetURL(host),
		elastic.SetSniff(false),
	)
	if err != nil {
		fmt.Println("Error connecting to ElasticSearch")
		panic(err)
	}
	fmt.Println("Connected to ES")

	termQuery := elastic.NewMatchAllQuery()
	aggLstUp := elastic.NewMaxAggregation().Field("sources.last_updated")
	aggCtAt := elastic.NewMaxAggregation().Field("sources.created_at")
	searchResults, err := client.Search().Index(indexName).Query(termQuery).Aggregation("max_last_updated", aggLstUp).Aggregation("max_created", aggCtAt).Do(ctx)
	if err != nil {
		panic(err)
	}

	maxLastUpdated, found := searchResults.Aggregations.MaxBucket("max_last_updated")
	if !found {
		panic(err)
	}
	fmt.Println("Max last updated", maxLastUpdated.ValueAsString)
	tm := int64(*maxLastUpdated.Value) / 1000
	mu := time.Unix(tm, 0)

	maxCreated, found := searchResults.Aggregations.MaxBucket("max_created")
	if !found {
		fmt.Println("max created not found")
	}
	fmt.Println("Max Created", maxCreated.ValueAsString)
	tm = int64(*maxCreated.Value) / 1000
	mc := time.Unix(tm, 0)
	oldest := mu
	if mc.Before(mu) {
		oldest = mc
	}

	m := fmt.Sprint("Oldest date: ", oldest)
	fmt.Println(m)

	fmt.Println("OLDEST", oldest)
}

func testUpsert(){
	var err error
	ctx := context.Background()
	indexName := "testindex"
	//        "type": "nested",
	//        "include_in_root": true,
	mapping := `
{
  "mappings": {
    "properties": {
      "el_nesteado": {
        "properties": {
          "id": {
            "type": "keyword",
			"copy_to": "nested_id"
          },
          "description": {
            "type": "keyword",
			"copy_to": "n_description"
          }
        }
      }
    }
  }
}
`
	host := "http://localhost:9200"
	client, err := elastic.NewClient(
		elastic.SetURL(host),
		elastic.SetSniff(false),
	)
	if err != nil {
		panic(err)
	}

	inf, code, err := client.Ping(host).Do(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Succesfully pinged ElasticSearch server with code %d and version %s \n", code, inf.Version.Number)

	ex, err := client.IndexExists(indexName).Do(ctx)
	if err != nil {
		panic(err)
	}

	if !ex {
		in, err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx)
		fmt.Printf("Creating index %s \n", indexName)
		if err != nil {
			panic(err)
		}
		if !in.Acknowledged {
			panic(err)
		}
		// Giving ES time to set up the Index
		time.Sleep(2 * time.Second)
	} else {
		fmt.Printf("Index %s exist, skipping its creation \n", indexName)
	}
	var nesteados []Elnesteado
	nesteados = append(nesteados, Elnesteado{
		Id:          "1",
		Description: "El primero ",
	})

	nesteados = append(nesteados, Elnesteado{
		Id:          "2",
		Description: "El segundo",
	})

	nesteados = append(nesteados, Elnesteado{
		Id:          "3",
		Description: "Another one GDSL:KFJG:LDKJFG:LKDF",
	})

	nesteados = append(nesteados, Elnesteado{
		Id:          "4",
		Description: "cuarto cuarto",
	})

	testy := Testy{
		Name:      "Ricardo",
		Nesteados: nesteados,
	}

	//added, err := client.Update().Index(indexName).DocAsUpsert(true).Id("1").Doc(testy).Do(ctx)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("Added %s %s \n", added.Id, added.Type)

	bulkService := client.Bulk()
	t := elastic.NewBulkUpdateRequest().Index(indexName).DocAsUpsert(true).Id("1").Doc(testy)
	bulkService.Add(t)
	br, err := bulkService.Do(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println(br)
}