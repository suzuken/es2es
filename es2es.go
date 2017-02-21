package es2es

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"net/url"
	"reflect"
	"runtime"
	"sync"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"
)

var (
	ErrRecordsNothing = errors.New("no records to post to Elasticsearch")
	ErrQueryNoHits    = errors.New("empty match")
	ErrAggsNotMatch   = errors.New("empty aggs match")
)

// Searcher is es2es client
type Searcher struct {
	es     *elastic.Client
	Config *SearcherConfig
}

// IDer provides ID string. It's working as document of Elasticsearch.
type IDer interface {
	// ID is used for _id in Elasticsearch.
	ID() string
}

// Index provides index name for reading.
func (searcher *Searcher) Index() string {
	return searcher.Config.Index
}

// ToIndex provides index name for writing.
func (searcher *Searcher) ToIndex() string {
	return searcher.Config.ToIndex
}

// ESType provides default document type or writing and reading.
func (searcher *Searcher) ESType() string {
	return searcher.Config.ESType
}

// SearcherConfig is configuration of Searcher.
type SearcherConfig struct {
	// ESHost is host names of Elasticsearch nodes.
	ESHost []string

	// esurl is Elasticsearch hosts URL.
	// These have same order with ESHost.
	esurl []*url.URL

	Index   string // Index is default index name for reading
	ToIndex string // ToIndex is default index for writing.

	// ESType is default document type for reading and writing.
	// You may want to define mapping before inserting documents.
	//
	// see: https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping.html
	ESType string

	// Sniff enables sniffing for searching nodes.
	// Probably you'll be fine with sniffing, but some cases it doesn't works such as
	// working with Amazon Elasticsearch Services or accessing to docker container from
	// host environment. Sniff API returns nodes ip address to connect, but the given ip
	// is not verify if it's can connect from your environment.
	//
	// see: https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html
	Sniff bool
}

// NewSearcherConfig returns searcher configuration.
//
// This exits process when host is not valid URL.
func NewSearcherConfig(esHost []string, index, toIndex, esType string, sniff bool) *SearcherConfig {
	esurl := make([]*url.URL, 0, len(esHost))
	for _, h := range esHost {
		u, err := url.Parse(h)
		if err != nil {
			log.Fatalf("specified Elasticsearch hosts is not valid URL.: %s", err)
		}
		esurl = append(esurl, u)
	}
	return &SearcherConfig{
		ESHost:  esHost,
		Index:   index,
		ToIndex: toIndex,
		ESType:  esType,
		Sniff:   sniff,
		esurl:   esurl,
	}
}

// URL returns one of host name from Elasticesearch at random.
func (c *SearcherConfig) URL() *url.URL {
	if len(c.esurl) == 0 {
		return nil
	}
	return c.esurl[rand.Intn(len(c.esurl))]
}

// NewSearcher returns Searcher object.
// This fails cannot connect Elasticsearch nodes.
func NewSearcher(config *SearcherConfig) (*Searcher, error) {
	es, err := elastic.NewClient(elastic.SetURL(config.ESHost...), elastic.SetMaxRetries(5), elastic.SetSniff(config.Sniff))
	if err != nil {
		return nil, err
	}
	return &Searcher{
		es:     es,
		Config: config,
	}, nil
}

// bulk is helper function for bulk insertion.
// This make bulk request when number of actions over its limit.
func (searcher *Searcher) bulk(bulk *elastic.BulkService, limit int) error {
	if bulk.NumberOfActions() <= 0 || bulk.NumberOfActions() < limit {
		return nil
	}
	resp, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}
	log.Printf("%d docs created, %d docs updated, %d docs indexed", len(resp.Created()), len(resp.Updated()), len(resp.Indexed()))
	if resp.Failed() != nil {
		for _, f := range resp.Failed() {
			log.Printf("fail to put %s (_id), reason: %v", f.Id, f.Error)
		}
	}
	return nil
}

func (searcher *Searcher) transformLoop(searchchan <-chan *elastic.SearchResult, transformed chan<- IDer, workerCount int, transformFunc func(*IDer)) {
	var page IDer // for reflection
	var wg sync.WaitGroup
	limit := make(chan bool, workerCount) // limit goroutine number

loop:
	for {
		select {
		case sr, ok := <-searchchan:
			if !ok {
				log.Print("searcher is closed. stop transformer.")
				break loop
			}
			log.Printf("transform %d pages", len(sr.Hits.Hits))
			for _, item := range sr.Each(reflect.TypeOf(page)) {
				if p, ok := item.(IDer); ok {
					wg.Add(1)
					limit <- true
					go func(p IDer) {
						defer wg.Done()
						transformFunc(&p)
						transformed <- p
						<-limit
					}(p)
				}
			}
			wg.Wait()
		}
	}
	close(transformed)
}

// BulkRequestFactory uses for building bulk requests.
type BulkRequestFactory func(IDer) elastic.BulkableRequest

func (searcher *Searcher) bulkLoop(
	transformed <-chan IDer, errchan chan<- error, workerquit chan<- bool,
	bulkSize int, bulkFac BulkRequestFactory) {
	var m sync.Mutex
	bulk := searcher.es.Bulk().Index(searcher.ToIndex()).Type(searcher.ESType())
	for {
		select {
		case p, ok := <-transformed:
			if !ok {
				log.Print("transformer is closed. make bulk request for rest buffers.")
				// when transformed closed, force flush buffer
				if err := searcher.bulk(bulk, 0); err != nil {
					errchan <- err
				}
				workerquit <- true
			}
			m.Lock()
			bulk = bulk.Add(bulkFac(p))
			m.Unlock()
			if err := searcher.bulk(bulk, bulkSize); err != nil {
				errchan <- err
			}
		}
	}
}

// searchLoop yields documents from Elasticsearch.
//
// searchLoop works until query finished when reached EOF. scrollSize is used to define scroll size
// on /search api. Despite of scroll size, searchLoop works until search finished.
//
// see also: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
func (searcher *Searcher) searchLoop(query elastic.Query, searchchan chan<- *elastic.SearchResult, errchan chan<- error, scrollSize int) {
	scrollService := searcher.es.Scroll(searcher.Index()).Type(searcher.ESType()).Query(query).Size(scrollSize)
loop:
	for {
		searchResult, err := scrollService.Do(context.Background())
		if err == io.EOF {
			log.Print("reached end of stream. stop searcher.")
			break loop
		} else if err != nil {
			errchan <- err
		}
		scrollService.ScrollId(searchResult.ScrollId)
		log.Printf("query took %d milliseconds, total hits: %d, actual hists: %d", searchResult.TookInMillis, searchResult.TotalHits(), len(searchResult.Hits.Hits))
		searchchan <- searchResult
	}
	close(searchchan)
}

// Reindex does query and fetch documents, transfor them, and bulk insert into specified index.
//
// Reindex use searcher configuration for reading and writing. You can specify Index to read
// and ToIndex to write.
//
// Reindex spawn workers. This tool have 3 workers: transformer, bulk inserter, and searcher.
// At first searcher starts querying and fetching documents from Elasticsearch. Documents are
// sent to transformer via channel. After transforming, bulk inserter accepts documents and making
// bulk insert objects. When reaching 1 batch size for bulk insertion, do actual bulk insert
// requests to Elasticsearch. Transformer and bulk inserter working as goroutine. You can specify
// concurrency for each workers by bulkSize and workerCount.
//
// scrollSize used for querying. A scroll size is used for specifying a maximum number of documents of each /search request. bulkSize defines how many documents to insert by 1 bulk insert request.  workerCount defines a number of many goroutines to transform. You can control concurrencly for your workload.
func (searcher *Searcher) Reindex(query elastic.Query, scrollSize, bulkSize, workerCount int, transformFunc func(*IDer)) error {
	searchchan := make(chan *elastic.SearchResult)
	transformed := make(chan IDer)
	workerquit := make(chan bool)
	errchan := make(chan error)

	go searcher.transformLoop(searchchan, transformed, workerCount, transformFunc)
	go searcher.bulkLoop(transformed, errchan, workerquit, bulkSize, func(p IDer) elastic.BulkableRequest {
		return elastic.NewBulkIndexRequest().Index(searcher.ToIndex()).Doc(p).Id(p.ID())
	})
	// show job stats
	go func() {
		for {
			select {
			case err, ok := <-errchan:
				if ok {
					log.Printf("error %s", err)
				}
			case <-time.Tick(1 * time.Second):
				log.Printf("stats: num goroutine: %d", runtime.NumGoroutine())
			}
		}
	}()

	searcher.searchLoop(query, searchchan, errchan, scrollSize)

	<-workerquit
	return nil
}

// DoSearch do actual search.
func (searcher *Searcher) DoSearch(query *elastic.QueryStringQuery, size int) (*elastic.SearchResult, error) {
	return searcher.es.Search(searcher.Index()).Type(searcher.ESType()).Query(query).Size(size).Do(context.Background())
}

// QueryStringQuery builds query string query string.
// Pattern accepts Query string syntax of Elasticsearch.
//
// see: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax
//
// Fields is for specifying default field.
//
// Query string query is not faster when compared with search API, but it's useful for
// command line client.
//
// see also: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
func QueryStringQuery(query string) *elastic.QueryStringQuery {
	if query == "*" {
		return elastic.NewQueryStringQuery("*")
	}
	return elastic.NewQueryStringQuery(query)
}

// Refresh do request to force indexing.
//
// see: http://stackoverflow.com/questions/12935810/integration-test-elastic-search-timing-issue-document-not-found
func (searcher *Searcher) Refresh(index string) (*elastic.RefreshResult, error) {
	if exists, err := searcher.es.IndexExists(index).Do(context.Background()); err != nil {
		return nil, err
	} else if !exists {
		log.Printf("index %s is not exists. wait until create..", index)
		time.Sleep(1 * time.Second)
		return searcher.Refresh(index)
	}
	return searcher.es.Refresh(index).Do(context.Background())
}
