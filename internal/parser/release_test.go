package parser

import "testing"

func TestParseReleaseExtractsEmbeddedRecordingData(t *testing.T) {
	line := []byte(`{
		"id":"release-1",
		"title":"Release Title",
		"status":"Official",
		"date":"2020-01-15",
		"country":"US",
		"barcode":"123456789012",
		"packaging":"Jewel Case",
		"release-group":{"id":"rg-1","title":"Group Title"},
		"artist-credit":[{"name":"Artist Name","joinphrase":"","artist":{"id":"artist-1","name":"Artist Name"}}],
		"label-info":[{"catalog-number":"ABC-123","label":{"id":"label-1","name":"Label Name"}}],
		"text-representation":{"language":"eng","script":"Latn"},
		"media":[
			{
				"format":"CD",
				"position":1,
				"track-count":1,
				"tracks":[
					{
						"id":"track-1",
						"number":"1",
						"title":"Track Title",
						"length":234000,
						"position":1,
						"recording":{
							"id":"recording-1",
							"title":"Recording Title",
							"length":234000,
							"disambiguation":"",
							"video":false,
							"first-release-date":"2020-01-15",
							"artist-credit":[{"name":"Artist Name","joinphrase":"","artist":{"id":"artist-1","name":"Artist Name"}}],
							"isrcs":["USRC11234567"],
							"relations":[{"type":"wikidata","target-type":"url","url":{"resource":"https://www.wikidata.org/wiki/Q1"}}]
						}
					}
				]
			}
		],
		"relations":[{"type":"official homepage","target-type":"url","url":{"resource":"https://example.com/release"}}]
	}`)

	mutation, err := ParseRelease(line)
	if err != nil {
		t.Fatalf("ParseRelease returned error: %v", err)
	}

	if got := len(mutation.Releases); got != 1 {
		t.Fatalf("expected 1 release row, got %d", got)
	}
	if got := len(mutation.ReleaseMedia); got != 1 {
		t.Fatalf("expected 1 release_media row, got %d", got)
	}
	if got := len(mutation.Tracks); got != 1 {
		t.Fatalf("expected 1 track row, got %d", got)
	}
	if got := len(mutation.Recordings); got != 1 {
		t.Fatalf("expected 1 recording row, got %d", got)
	}
	if got := len(mutation.RecordingArtists); got != 1 {
		t.Fatalf("expected 1 recording artist row, got %d", got)
	}
	if got := len(mutation.RecordingISRCs); got != 1 {
		t.Fatalf("expected 1 recording isrc row, got %d", got)
	}
	if got := len(mutation.ExternalLinks); got != 2 {
		t.Fatalf("expected 2 external links, got %d", got)
	}
	if got := mutation.Releases[0].ReleaseGroupMBID; got != "rg-1" {
		t.Fatalf("unexpected release_group_mbid: %q", got)
	}
	if got := mutation.Tracks[0].RecordingMBID; got != "recording-1" {
		t.Fatalf("unexpected track recording mbid: %q", got)
	}
}
