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
		`INSERT INTO labels(mbid, name, sort_name, type, country) VALUES('label-1', 'Nothing Records', 'Nothing Records', 'Original Production', 'US')`,
		`INSERT INTO label_aliases(label_mbid, name, locale, is_primary) VALUES('label-1', 'Nothing', '', 1)`,
		`INSERT INTO works(mbid, title, type, languages) VALUES('work-1', 'Closer', 'Song', 'eng')`,
		`INSERT INTO work_aliases(work_mbid, name, locale, is_primary) VALUES('work-1', 'Closer (NIN song)', '', 0)`,
		`INSERT INTO work_iswcs(work_mbid, iswc) VALUES('work-1', 'T-070.971.184-9')`,
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

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM search_fts WHERE entity_type = 'label' AND search_fts MATCH 'nothing*'`).Scan(&count); err != nil {
		t.Fatalf("query label fts: %v", err)
	}
	if count != 1 {
		t.Fatalf("label fts matches: got %d want 1", count)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM search_fts WHERE entity_type = 'work' AND search_fts MATCH 'closer*'`).Scan(&count); err != nil {
		t.Fatalf("query work fts: %v", err)
	}
	if count != 1 {
		t.Fatalf("work fts matches: got %d want 1", count)
	}
}
