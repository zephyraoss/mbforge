package db

import (
	"context"
	"testing"

	"github.com/zephyraoss/mbforge/internal/libsqlutil"
)

func TestRebuildSearchIndexCreatesFTSMatches(t *testing.T) {
	ctx := context.Background()

	db, err := libsqlutil.OpenLocal(":memory:")
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	defer db.Close()

	if err := CreateSchema(ctx, db); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}

	stmts := []string{
		`INSERT INTO artists(mbid, name, sort_name, type, country) VALUES('artist-1', 'Nine Inch Nails', 'Nine Inch Nails', 'Group', 'US')`,
		`INSERT INTO artist_aliases(artist_mbid, name, locale, is_primary) VALUES('artist-1', 'NIN', '', 1)`,
		`INSERT INTO releases(mbid, title, date, country, barcode) VALUES('release-1', 'The Downward Spiral', '1994-03-08', 'US', '123456789012')`,
		`INSERT INTO release_artists(release_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('release-1', 'artist-1', 'Nine Inch Nails', '', 1)`,
		`INSERT INTO recordings(mbid, title, first_release_date) VALUES('recording-1', 'Closer', '1994-03-08')`,
		`INSERT INTO recording_artists(recording_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('recording-1', 'artist-1', 'Nine Inch Nails', '', 1)`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	if err := RebuildSearchIndex(ctx, db, nil); err != nil {
		t.Fatalf("RebuildSearchIndex: %v", err)
	}

	ok, err := SearchIndexExists(ctx, db)
	if err != nil {
		t.Fatalf("SearchIndexExists: %v", err)
	}
	if !ok {
		t.Fatalf("expected search index to exist")
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM search_fts WHERE search_fts MATCH 'nin*'`).Scan(&count); err != nil {
		t.Fatalf("query search_fts: %v", err)
	}
	if count == 0 {
		t.Fatalf("expected at least one FTS match")
	}
}
