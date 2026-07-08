package parser

import "testing"

func TestParseLabelExtractsFields(t *testing.T) {
	line := []byte(`{
		"id":"label-1",
		"name":"Campanella Musica",
		"sort-name":"Campanella Musica",
		"disambiguation":"German classical label",
		"type":"Original Production",
		"label-code":10298,
		"country":"DE",
		"area":{"id":"area-de","name":"Germany"},
		"life-span":{"begin":"1990","end":null,"ended":false},
		"aliases":[
			{"name":"Campanella","sort-name":"Campanella","type":"Label name","locale":"de","primary":true},
			{"name":"Campanella","sort-name":"Campanella","type":"Label name","locale":"de","primary":true}
		],
		"tags":[{"name":"classical","count":3},{"name":"classical","count":3}],
		"genres":[{"id":"genre-1","name":"classical","count":3}],
		"relations":[{"type":"official site","target-type":"url","url":{"resource":"https://example.com/label"}}]
	}`)

	mutation, err := ParseLabel(line)
	if err != nil {
		t.Fatalf("ParseLabel returned error: %v", err)
	}

	if got := len(mutation.Labels); got != 1 {
		t.Fatalf("expected 1 label row, got %d", got)
	}
	label := mutation.Labels[0]
	if label.MBID != "label-1" || label.Name != "Campanella Musica" {
		t.Fatalf("unexpected label row: %+v", label)
	}
	if label.LabelCode == nil || *label.LabelCode != 10298 {
		t.Fatalf("unexpected label code: %v", label.LabelCode)
	}
	if label.Country != "DE" || label.AreaMBID != "area-de" || label.AreaName != "Germany" {
		t.Fatalf("unexpected country/area: %+v", label)
	}
	if label.BeginDate != "1990" || label.EndDate != "" || label.Ended {
		t.Fatalf("unexpected life-span: %+v", label)
	}
	if got := len(mutation.LabelAliases); got != 1 {
		t.Fatalf("expected 1 deduped alias row, got %d", got)
	}
	if got := len(mutation.LabelTags); got != 1 {
		t.Fatalf("expected 1 deduped tag row, got %d", got)
	}
	if got := len(mutation.LabelGenres); got != 1 {
		t.Fatalf("expected 1 genre row, got %d", got)
	}
	if got := len(mutation.ExternalLinks); got != 1 {
		t.Fatalf("expected 1 external link, got %d", got)
	}
	if got := mutation.ExternalLinks[0].EntityType; got != "label" {
		t.Fatalf("unexpected external link entity type: %q", got)
	}
}

func TestParseLabelNullCodeAndMissingSortName(t *testing.T) {
	line := []byte(`{"id":"label-2","name":"Sur Muzique","sort-name":"","label-code":null}`)

	mutation, err := ParseLabel(line)
	if err != nil {
		t.Fatalf("ParseLabel returned error: %v", err)
	}
	label := mutation.Labels[0]
	if label.LabelCode != nil {
		t.Fatalf("expected nil label code, got %v", *label.LabelCode)
	}
	if label.SortName != "Sur Muzique" {
		t.Fatalf("expected sort name to fall back to name, got %q", label.SortName)
	}
}
