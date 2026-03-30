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
