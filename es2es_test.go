package es2es

import (
	"testing"
)

type Page struct {
}

func (p *Page) ID() string {
	return ""
}

func TestSearcherConfigURL(t *testing.T) {
	u := "http://localhost:9200"
	c := NewSearcherConfig([]string{u}, "", "", "", true)
	if c.URL().String() != u {
		t.Fatalf("taken url should be %s but not: actual %s", u, c.URL().String())
	}
}
