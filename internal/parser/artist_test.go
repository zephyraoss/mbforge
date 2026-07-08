package parser

import "testing"

func TestParseArtistExtractsRelationships(t *testing.T) {
	line := []byte(`{
		"id":"artist-1",
		"name":"Band Member",
		"sort-name":"Member, Band",
		"relations":[
			{
				"type":"member of band",
				"target-type":"artist",
				"direction":"backward",
				"begin":"1975",
				"end":"1981",
				"ended":true,
				"attributes":["saxophone","flute"],
				"artist":{"id":"band-1","name":"The Band","sort-name":"Band, The"}
			},
			{
				"type":"member of band",
				"target-type":"artist",
				"direction":"backward",
				"begin":"1975",
				"end":"1981",
				"ended":true,
				"attributes":["flute","saxophone"],
				"artist":{"id":"band-1","name":"The Band","sort-name":"Band, The"}
			},
			{
				"type":"member of band",
				"target-type":"artist",
				"direction":"backward",
				"begin":"1994",
				"end":"1995",
				"ended":true,
				"attributes":["flute","saxophone"],
				"artist":{"id":"band-1","name":"The Band","sort-name":"Band, The"}
			},
			{
				"type":"official homepage",
				"target-type":"url",
				"url":{"resource":"https://example.com"}
			}
		]
	}`)

	mutation, err := ParseArtist(line)
	if err != nil {
		t.Fatalf("ParseArtist returned error: %v", err)
	}

	if got := len(mutation.ArtistRelationships); got != 2 {
		t.Fatalf("expected 2 relationship rows, got %d", got)
	}
	rel := mutation.ArtistRelationships[0]
	if rel.ArtistMBID != "artist-1" || rel.RelatedArtistMBID != "band-1" {
		t.Fatalf("unexpected relationship endpoints: %+v", rel)
	}
	if rel.Type != "member of band" || rel.Direction != "backward" {
		t.Fatalf("unexpected relationship type/direction: %+v", rel)
	}
	if rel.BeginDate != "1975" || rel.EndDate != "1981" || !rel.Ended {
		t.Fatalf("unexpected relationship period: %+v", rel)
	}
	if rel.RelatedArtistName != "The Band" {
		t.Fatalf("unexpected related artist name: %q", rel.RelatedArtistName)
	}
	if rel.Attributes != `["flute","saxophone"]` {
		t.Fatalf("expected sorted JSON attributes, got %q", rel.Attributes)
	}
	if got := len(mutation.ExternalLinks); got != 1 {
		t.Fatalf("expected 1 external link, got %d", got)
	}
}
