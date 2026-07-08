package parser

import "testing"

func TestParseWorkExtractsFieldsAndRecordingLinks(t *testing.T) {
	line := []byte(`{
		"id":"work-1",
		"title":"Suck",
		"disambiguation":"NIN song",
		"type":"Song",
		"language":"eng",
		"languages":["eng","deu"],
		"iswcs":["T-070.172.108-5","T-070.172.108-5","T-900.245.777-4"],
		"aliases":[{"name":"Suck (live)","sort-name":"Suck (live)","locale":"","primary":false}],
		"tags":[{"name":"industrial","count":2}],
		"relations":[
			{
				"type":"performance",
				"target-type":"recording",
				"direction":"backward",
				"attributes":["live","cover"],
				"recording":{"id":"rec-1"}
			},
			{
				"type":"performance",
				"target-type":"recording",
				"direction":"backward",
				"attributes":[],
				"recording":{"id":"rec-2"}
			},
			{
				"type":"composer",
				"target-type":"artist",
				"direction":"backward",
				"artist":{"id":"artist-1","name":"Composer"}
			},
			{"type":"wikidata","target-type":"url","url":{"resource":"https://www.wikidata.org/wiki/Q1"}}
		]
	}`)

	mutation, err := ParseWork(line)
	if err != nil {
		t.Fatalf("ParseWork returned error: %v", err)
	}

	if got := len(mutation.Works); got != 1 {
		t.Fatalf("expected 1 work row, got %d", got)
	}
	work := mutation.Works[0]
	if work.MBID != "work-1" || work.Title != "Suck" || work.Type != "Song" {
		t.Fatalf("unexpected work row: %+v", work)
	}
	if work.Languages != "eng,deu" {
		t.Fatalf("unexpected languages: %q", work.Languages)
	}
	if got := len(mutation.WorkISWCs); got != 2 {
		t.Fatalf("expected 2 deduped iswc rows, got %d", got)
	}
	if got := len(mutation.WorkAliases); got != 1 {
		t.Fatalf("expected 1 alias row, got %d", got)
	}
	if got := len(mutation.WorkTags); got != 1 {
		t.Fatalf("expected 1 tag row, got %d", got)
	}
	if got := len(mutation.RecordingWorks); got != 2 {
		t.Fatalf("expected 2 recording links, got %d", got)
	}
	link := mutation.RecordingWorks[0]
	if link.RecordingMBID != "rec-1" || link.WorkMBID != "work-1" || link.Type != "performance" {
		t.Fatalf("unexpected recording link: %+v", link)
	}
	if link.Attributes != `["cover","live"]` {
		t.Fatalf("expected sorted JSON attributes, got %q", link.Attributes)
	}
	if got := mutation.RecordingWorks[1].Attributes; got != "" {
		t.Fatalf("expected empty attributes, got %q", got)
	}
	if got := len(mutation.ExternalLinks); got != 1 {
		t.Fatalf("expected 1 external link, got %d", got)
	}
	if got := mutation.ExternalLinks[0].EntityType; got != "work" {
		t.Fatalf("unexpected external link entity type: %q", got)
	}
}

func TestParseWorkLanguageFallback(t *testing.T) {
	line := []byte(`{"id":"work-2","title":"Untitled","language":"zxx","languages":[]}`)

	mutation, err := ParseWork(line)
	if err != nil {
		t.Fatalf("ParseWork returned error: %v", err)
	}
	if got := mutation.Works[0].Languages; got != "zxx" {
		t.Fatalf("expected language fallback, got %q", got)
	}
}
