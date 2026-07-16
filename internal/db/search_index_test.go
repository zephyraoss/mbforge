package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/zephyraoss/mbforge/internal/libsqlutil"
)

func newSearchIndexTestDB(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()

	db, err := libsqlutil.OpenLocal(":memory:")
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if err := CreateSchema(ctx, db); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}

	stmts := []string{
		`INSERT INTO artists(mbid, name, sort_name, type, country) VALUES('artist-1', 'Nine Inch Nails', 'Nine Inch Nails', 'Group', 'US')`,
		`INSERT INTO artist_aliases(artist_mbid, name, locale, is_primary) VALUES('artist-1', 'NIN', '', 1)`,
		`INSERT INTO release_groups(mbid, title, primary_type, first_release_date) VALUES('rg-1', 'The Downward Spiral', 'Album', '1994-03-08')`,
		`INSERT INTO release_group_artists(release_group_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('rg-1', 'artist-1', 'Nine Inch Nails', '', 1)`,
		`INSERT INTO releases(mbid, title, date, country, barcode) VALUES('release-1', 'The Downward Spiral', '1994-03-08', 'US', '123456789012')`,
		`INSERT INTO release_artists(release_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('release-1', 'artist-1', 'Nine Inch Nails', '', 1)`,
		`INSERT INTO recordings(mbid, title, first_release_date) VALUES('recording-1', 'Closer', '1994-03-08')`,
		`INSERT INTO recording_artists(recording_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('recording-1', 'artist-1', 'Nine Inch Nails', '', 1)`,
		`INSERT INTO tracks(mbid, release_mbid, recording_mbid, media_position, position, number, title) VALUES('track-1', 'release-1', 'recording-1', 1, 9, '9', 'Closer')`,
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
	return db
}

func countFTSRowsByType(t *testing.T, db *sql.DB) map[string]int {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT entity_type, COUNT(*) FROM search_fts GROUP BY entity_type`)
	if err != nil {
		t.Fatalf("count search_fts rows: %v", err)
	}
	defer rows.Close()

	out := make(map[string]int)
	for rows.Next() {
		var entityType string
		var count int
		if err := rows.Scan(&entityType, &count); err != nil {
			t.Fatalf("scan count: %v", err)
		}
		out[entityType] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return out
}

func readMetaValue(t *testing.T, db *sql.DB, key string) string {
	t.Helper()
	var value string
	if err := db.QueryRowContext(context.Background(), `SELECT value FROM _meta WHERE key = ?`, key).Scan(&value); err != nil {
		t.Fatalf("read _meta %s: %v", key, err)
	}
	return value
}

func TestRebuildSearchIndexWithTracks(t *testing.T) {
	ctx := context.Background()
	db := newSearchIndexTestDB(t)

	if err := RebuildSearchIndex(ctx, db, SearchIndexOptions{IncludeTracks: true}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex: %v", err)
	}

	ok, err := SearchIndexExists(ctx, db)
	if err != nil {
		t.Fatalf("SearchIndexExists: %v", err)
	}
	if !ok {
		t.Fatalf("expected search index to exist")
	}

	want := map[string]int{
		"artist":        1,
		"label":         1,
		"work":          1,
		"release_group": 1,
		"release":       1,
		"recording":     1,
		"track":         1,
	}
	got := countFTSRowsByType(t, db)
	for entityType, count := range want {
		if got[entityType] != count {
			t.Fatalf("fts rows for %s: got %d want %d", entityType, got[entityType], count)
		}
	}

	if value := readMetaValue(t, db, MetaKeySearchIndexTracks); value != "true" {
		t.Fatalf("_meta %s: got %q want %q", MetaKeySearchIndexTracks, value, "true")
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

func TestRebuildSearchIndexWithoutTracks(t *testing.T) {
	ctx := context.Background()
	db := newSearchIndexTestDB(t)

	if err := RebuildSearchIndex(ctx, db, SearchIndexOptions{IncludeTracks: true}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex with tracks: %v", err)
	}
	if err := RebuildSearchIndex(ctx, db, SearchIndexOptions{IncludeTracks: false}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex without tracks: %v", err)
	}

	got := countFTSRowsByType(t, db)
	if got["track"] != 0 {
		t.Fatalf("fts rows for track: got %d want 0", got["track"])
	}
	for _, entityType := range []string{"artist", "label", "work", "release_group", "release", "recording"} {
		if got[entityType] != 1 {
			t.Fatalf("fts rows for %s: got %d want 1", entityType, got[entityType])
		}
	}

	if value := readMetaValue(t, db, MetaKeySearchIndexTracks); value != "false" {
		t.Fatalf("_meta %s: got %q want %q", MetaKeySearchIndexTracks, value, "false")
	}
}

func TestSearchIndexIncludesTracks(t *testing.T) {
	if !SearchIndexIncludesTracks(map[string]string{}) {
		t.Fatalf("missing key should default to including tracks")
	}
	if !SearchIndexIncludesTracks(map[string]string{MetaKeySearchIndexTracks: "true"}) {
		t.Fatalf("explicit true should include tracks")
	}
	if SearchIndexIncludesTracks(map[string]string{MetaKeySearchIndexTracks: "false"}) {
		t.Fatalf("explicit false should exclude tracks")
	}
}

func readMapEntries(t *testing.T, db *sql.DB) map[string]int64 {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT entity_type, entity_mbid, fts_rowid FROM search_fts_map`)
	if err != nil {
		t.Fatalf("read search_fts_map: %v", err)
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var entityType, mbid string
		var rowid int64
		if err := rows.Scan(&entityType, &mbid, &rowid); err != nil {
			t.Fatalf("scan map entry: %v", err)
		}
		out[entityType+"/"+mbid] = rowid
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("map rows: %v", err)
	}
	return out
}

func readFTSEntries(t *testing.T, db *sql.DB) map[string]int64 {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT entity_type, entity_mbid, rowid FROM search_fts`)
	if err != nil {
		t.Fatalf("read search_fts: %v", err)
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var entityType, mbid string
		var rowid int64
		if err := rows.Scan(&entityType, &mbid, &rowid); err != nil {
			t.Fatalf("scan fts entry: %v", err)
		}
		out[entityType+"/"+mbid] = rowid
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("fts rows: %v", err)
	}
	return out
}

func requireMapMatchesFTS(t *testing.T, db *sql.DB) {
	t.Helper()
	fts := readFTSEntries(t, db)
	mapped := readMapEntries(t, db)
	if len(fts) != len(mapped) {
		t.Fatalf("map has %d entries, fts has %d", len(mapped), len(fts))
	}
	for key, rowid := range fts {
		if mapped[key] != rowid {
			t.Fatalf("map entry %s = rowid %d, fts has rowid %d", key, mapped[key], rowid)
		}
	}
}

func TestRefreshSearchIndexRowsMaintainsMap(t *testing.T) {
	ctx := context.Background()
	db := newSearchIndexTestDB(t)
	if err := RebuildSearchIndex(ctx, db, SearchIndexOptions{IncludeTracks: true}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex: %v", err)
	}
	requireMapMatchesFTS(t, db)

	stmts := []string{
		`UPDATE artists SET name = 'Renamed Artist' WHERE mbid = 'artist-1'`,
		`DELETE FROM label_aliases WHERE label_mbid = 'label-1'`,
		`DELETE FROM labels WHERE mbid = 'label-1'`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	changed := map[string][]string{
		"artist": {"artist-1"},
		"label":  {"label-1"},
	}
	if err := RefreshSearchIndexRows(ctx, tx, changed, true); err != nil {
		t.Fatalf("RefreshSearchIndexRows: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	fts := readFTSEntries(t, db)
	if _, ok := fts["label/label-1"]; ok {
		t.Fatal("deleted label still present in search_fts")
	}
	var heading string
	if err := db.QueryRowContext(ctx, `SELECT heading FROM search_fts WHERE rowid = ?`, fts["artist/artist-1"]).Scan(&heading); err != nil {
		t.Fatalf("read refreshed artist row: %v", err)
	}
	if heading != "Renamed Artist" {
		t.Fatalf("refreshed artist heading = %q", heading)
	}
	requireMapMatchesFTS(t, db)
}

func TestEnsureSearchIndexMapMigratesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := newSearchIndexTestDB(t)
	if err := RebuildSearchIndex(ctx, db, SearchIndexOptions{IncludeTracks: true}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE search_fts_map`); err != nil {
		t.Fatalf("drop map: %v", err)
	}

	if err := EnsureSearchIndexMap(ctx, db, nil); err != nil {
		t.Fatalf("EnsureSearchIndexMap: %v", err)
	}
	requireMapMatchesFTS(t, db)

	before := readMapEntries(t, db)
	if err := EnsureSearchIndexMap(ctx, db, nil); err != nil {
		t.Fatalf("EnsureSearchIndexMap second call: %v", err)
	}
	after := readMapEntries(t, db)
	if len(before) != len(after) {
		t.Fatalf("second EnsureSearchIndexMap changed the map: %d -> %d entries", len(before), len(after))
	}
}
