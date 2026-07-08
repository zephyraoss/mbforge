package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/zephyraoss/mbforge/internal/libsqlutil"
	"github.com/zephyraoss/mbforge/internal/model"
)

func TestWriterSplitsLargeBatchByVariableLimit(t *testing.T) {
	ctx := context.Background()

	db, err := libsqlutil.OpenLocal(":memory:")
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	defer db.Close()

	if err := CreateSchema(ctx, db); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}

	writer, err := NewWriter(ctx, db, 5000)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Rollback()

	rows := make([]model.ArtistRow, 0, 3000)
	for i := 0; i < 3000; i++ {
		rows = append(rows, model.ArtistRow{
			MBID:     fmt.Sprintf("artist-%d", i),
			Name:     fmt.Sprintf("Artist %d", i),
			SortName: fmt.Sprintf("Artist %d", i),
		})
	}

	if err := writer.WriteMutation(model.Mutation{Artists: rows}); err != nil {
		t.Fatalf("WriteMutation: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM artists`).Scan(&count); err != nil {
		t.Fatalf("count artists: %v", err)
	}
	if count != 3000 {
		t.Fatalf("unexpected artist count: got %d want 3000", count)
	}
}

func TestWriterRoundTripsNewEntityRows(t *testing.T) {
	ctx := context.Background()

	db, err := libsqlutil.OpenLocal(":memory:")
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	defer db.Close()

	if err := ApplyBuildPragmas(ctx, db); err != nil {
		t.Fatalf("ApplyBuildPragmas: %v", err)
	}
	if err := CreateSchema(ctx, db); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}

	writer, err := NewWriter(ctx, db, 5000)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Rollback()

	code := 42
	mutation := model.Mutation{
		Labels:       []model.LabelRow{{MBID: "l1", Name: "Label", SortName: "Label", LabelCode: &code, Country: "US"}},
		LabelAliases: []model.LabelAliasRow{{LabelMBID: "l1", Name: "Alias"}},
		LabelTags:    []model.LabelTagRow{{LabelMBID: "l1", Tag: "rock", Count: 1}},
		LabelGenres:  []model.LabelGenreRow{{LabelMBID: "l1", GenreMBID: "g1", GenreName: "rock", Count: 1}},
		Works:        []model.WorkRow{{MBID: "w1", Title: "Work", Type: "Song", Languages: "eng"}},
		WorkAliases:  []model.WorkAliasRow{{WorkMBID: "w1", Name: "Alias"}},
		WorkISWCs:    []model.WorkISWCRow{{WorkMBID: "w1", ISWC: "T-000.000.001-0"}},
		WorkTags:     []model.WorkTagRow{{WorkMBID: "w1", Tag: "song", Count: 1}},
		RecordingWorks: []model.RecordingWorkRow{
			{RecordingMBID: "rec1", WorkMBID: "w1", Type: "performance", Attributes: `["live"]`},
		},
		ArtistRelationships: []model.ArtistRelationshipRow{
			{ArtistMBID: "a1", RelatedArtistMBID: "a2", RelatedArtistName: "Other", Type: "member of band", Direction: "backward", BeginDate: "1990", Ended: true, Attributes: `["guitar"]`},
		},
	}
	if err := writer.WriteMutation(mutation); err != nil {
		t.Fatalf("WriteMutation: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for table, want := range map[string]int{
		"labels":               1,
		"label_aliases":        1,
		"label_tags":           1,
		"label_genres":         1,
		"works":                1,
		"work_aliases":         1,
		"work_iswcs":           1,
		"work_tags":            1,
		"recording_works":      1,
		"artist_relationships": 1,
	} {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != want {
			t.Fatalf("%s count: got %d want %d", table, count, want)
		}
	}
}

func TestMaxRowsPerInsertRespectsSQLiteVariableLimit(t *testing.T) {
	spec := tableSpec{
		table:   "artists",
		columns: []string{"mbid", "name", "sort_name", "disambiguation", "type", "country", "gender", "begin_date", "end_date", "ended", "area_mbid", "area_name"},
	}

	got := maxRowsPerInsert(spec)
	if got != 2730 {
		t.Fatalf("unexpected max rows: got %d want 2730", got)
	}
}
