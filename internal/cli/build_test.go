package cli

import (
	"reflect"
	"testing"
)

func TestParseEntityListDefaultsToAll(t *testing.T) {
	got, err := parseEntityList("")
	if err != nil {
		t.Fatalf("parseEntityList returned error: %v", err)
	}

	want := []string{"artist", "release-group", "release", "recording"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseEntityList mismatch: got %v want %v", got, want)
	}
}

func TestParseEntityListSortsAndDedupes(t *testing.T) {
	got, err := parseEntityList("recording,artist,recording")
	if err != nil {
		t.Fatalf("parseEntityList returned error: %v", err)
	}

	want := []string{"artist", "recording"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseEntityList mismatch: got %v want %v", got, want)
	}
}
