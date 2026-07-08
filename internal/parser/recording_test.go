package parser

import "testing"

func TestParseRecordingExtractsWorkLinks(t *testing.T) {
	line := []byte(`{
		"id":"rec-1",
		"title":"Suck (live)",
		"length":251000,
		"relations":[
			{
				"type":"performance",
				"target-type":"work",
				"direction":"forward",
				"attributes":["live","cover"],
				"work":{
					"id":"work-1",
					"title":"Suck",
					"type":"Song",
					"language":"eng",
					"languages":["eng"],
					"iswcs":["T-070.172.108-5"]
				}
			},
			{
				"type":"performance",
				"target-type":"work",
				"direction":"forward",
				"attributes":["cover","live"],
				"work":{"id":"work-1","title":"Suck"}
			},
			{"type":"wikidata","target-type":"url","url":{"resource":"https://www.wikidata.org/wiki/Q1"}}
		]
	}`)

	mutation, err := ParseRecording(line)
	if err != nil {
		t.Fatalf("ParseRecording returned error: %v", err)
	}

	if got := len(mutation.RecordingWorks); got != 1 {
		t.Fatalf("expected 1 deduped work link, got %d", got)
	}
	link := mutation.RecordingWorks[0]
	if link.RecordingMBID != "rec-1" || link.WorkMBID != "work-1" || link.Type != "performance" {
		t.Fatalf("unexpected work link: %+v", link)
	}
	if link.Attributes != `["cover","live"]` {
		t.Fatalf("expected sorted JSON attributes, got %q", link.Attributes)
	}

	if got := len(mutation.Works); got != 1 {
		t.Fatalf("expected 1 embedded work row, got %d", got)
	}
	work := mutation.Works[0]
	if work.MBID != "work-1" || work.Title != "Suck" || work.Type != "Song" || work.Languages != "eng" {
		t.Fatalf("unexpected embedded work: %+v", work)
	}
	if got := len(mutation.WorkISWCs); got != 1 {
		t.Fatalf("expected 1 embedded iswc row, got %d", got)
	}
	if got := len(mutation.ExternalLinks); got != 1 {
		t.Fatalf("expected 1 external link, got %d", got)
	}
}
