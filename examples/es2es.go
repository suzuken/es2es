package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/suzuken/es2es"
)

// ESHosts is type for command-line flag.
// This accepts multiple Elasticsearch host name.
type ESHosts []string

func (e *ESHosts) String() string {
	return fmt.Sprint(*e)
}

// Set is method for FlagSet.
// ESHosts accepts comma separated host names of Elasticsearch nodes.
func (e *ESHosts) Set(value string) error {
	if len(*e) > 0 {
		return errors.New("eshosts flag already set")
	}
	for _, host := range strings.Split(value, ",") {
		*e = append(*e, strings.TrimSpace(host))
	}
	return nil
}

var esHostsFlag ESHosts

func init() {
	flag.Var(&esHostsFlag, "eshosts", "comma-separated list of Elasticsearch hosts. for example: http://server1:9200,http://server2:9200")
}

func printUsage() {
	flag.Usage()
	os.Exit(1)
}

func main() {
	var (
		fromindex   = flag.String("fromindex", "test-index", "Elasticsearch index name for retrieving")
		toindex     = flag.String("toindex", "test-index-new", "Elasticsearch index name for inserting")
		esType      = flag.String("type", "test-doc", "Elasticsearch default type name for inserting")
		sniff       = flag.Bool("sniff", true, "discovery nodes using sniff or not")
		scrollSize  = flag.Int("scrollsize", 200, "scroll size to fetch records per shard")
		bulkSize    = flag.Int("bulksize", 10, "a number of operation per bulk request")
		query       = flag.String("query", "*", "query is used for filtering documents by query string query. when using preview option, you can get how many documents will be transformed.")
		preview     = flag.Bool("preview", false, "dry-run mode: preview how many documents will be reindexd.")
		workerCount = flag.Int("worker", 100, "number of goroutine to transform page")
	)

	flag.Parse()

	if esHostsFlag == nil {
		fmt.Fprint(os.Stderr, "Missing elasticsearch host address (\"-help\" for help)")
		os.Exit(1)
	}

	config := es2es.NewSearcherConfig(esHostsFlag, *fromindex, *toindex, *esType, *sniff)
	searcher, err := es2es.NewSearcher(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail to initialize client %s", err)
		os.Exit(1)
	}

	if *preview {
		ret, err := searcher.DoSearch(es2es.QueryStringQuery(*query), 10)
		if err != nil {
			fmt.Fprintf(os.Stderr, "preview failed: %s", err)
			os.Exit(1)
		}
		fmt.Printf("search preview: %d docs will be reindexed (not transformed yet)\n", ret.Hits.TotalHits)
		os.Exit(0)
	}

	if err := searcher.Reindex(es2es.QueryStringQuery(*query), *scrollSize, *bulkSize, *workerCount, func(p *es2es.IDer) {
		// Do nothing
		return
	}); err != nil {
		fmt.Fprintf(os.Stderr, "fail to reindex %s", err)
		os.Exit(1)
	}
}
