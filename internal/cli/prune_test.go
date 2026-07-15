package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	mbdb "github.com/zephyraoss/mbforge/internal/db"
	"github.com/zephyraoss/mbforge/internal/libsqlutil"
)

func TestRunPruneAppliesFixturePacket(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "musicbrainz.db")

	db, err := libsqlutil.OpenLocal(dbPath)
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	if err := mbdb.CreateSchema(ctx, db); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	stmts := []string{
		`INSERT INTO artists(mbid, name, sort_name) VALUES('8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1', 'Merged Away', 'Away, Merged')`,
		`INSERT INTO artist_aliases(artist_mbid, name, locale, is_primary) VALUES('8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1', 'MA', '', 1)`,
		`INSERT INTO artists(mbid, name, sort_name) VALUES('6f0c2c16-dd7e-4268-a484-bfbcb2d4c48b', 'Merge Target', 'Target, Merge')`,
		`INSERT INTO recordings(mbid, title) VALUES('0a8e8d55-4b83-4d93-b31c-b7c9f8f5aeae', 'Deleted Song')`,
		`INSERT INTO recording_artists(recording_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('0a8e8d55-4b83-4d93-b31c-b7c9f8f5aeae', '6f0c2c16-dd7e-4268-a484-bfbcb2d4c48b', 'Merge Target', '', 1)`,
		`INSERT INTO _meta(key, value) VALUES('replication_sequence', '170431')`,
		`INSERT INTO _meta(key, value) VALUES('schema_sequence', '30')`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}
	if err := mbdb.RebuildSearchIndex(ctx, db, mbdb.SearchIndexOptions{IncludeTracks: false}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	fixture, err := os.ReadFile(filepath.Join("..", "replication", "testdata", "replication-170432-v2.tar.bz2"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("token") != "test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/replication-info":
			w.Write([]byte(`{"last_packet": "replication-170432.tar.bz2"}`))
		case "/replication-170432-v2.tar.bz2":
			w.Write(fixture)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := pruneConfig{
		DBPath:        dbPath,
		Token:         "test-token",
		BaseURL:       server.URL,
		DumpDir:       filepath.Join(tmp, "mbdump"),
		ResolveRemote: false,
	}
	if err := runPrune(ctx, cfg); err != nil {
		t.Fatalf("runPrune: %v", err)
	}

	db, err = libsqlutil.OpenLocal(dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()
	if rows, err := db.QueryContext(ctx, `PRAGMA busy_timeout = 30000`); err != nil {
		t.Fatalf("set busy_timeout: %v", err)
	} else {
		rows.Close()
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM artists WHERE mbid = '8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1'`).Scan(&count); err != nil {
		t.Fatalf("count merged artist: %v", err)
	}
	if count != 0 {
		t.Errorf("merged artist still present")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM artist_aliases WHERE artist_mbid = '8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1'`).Scan(&count); err != nil {
		t.Fatalf("count merged artist aliases: %v", err)
	}
	if count != 0 {
		t.Errorf("merged artist aliases still present")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM recordings`).Scan(&count); err != nil {
		t.Fatalf("count recordings: %v", err)
	}
	if count != 0 {
		t.Errorf("deleted recording still present")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM artists WHERE mbid = '6f0c2c16-dd7e-4268-a484-bfbcb2d4c48b'`).Scan(&count); err != nil {
		t.Fatalf("count target artist: %v", err)
	}
	if count != 1 {
		t.Errorf("merge target artist missing")
	}

	var newMBID string
	if err := db.QueryRowContext(ctx, `SELECT new_mbid FROM gid_redirects WHERE entity_type = 'artist' AND old_mbid = '8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1'`).Scan(&newMBID); err != nil {
		t.Fatalf("read artist redirect: %v", err)
	}
	if newMBID != "6f0c2c16-dd7e-4268-a484-bfbcb2d4c48b" {
		t.Errorf("artist redirect target: got %q", newMBID)
	}

	pending, err := mbdb.ListPendingRedirects(ctx, db, 10)
	if err != nil {
		t.Fatalf("ListPendingRedirects: %v", err)
	}
	if len(pending) != 1 || pending[0].EntityType != "release_group" || pending[0].NewRowID != 88001 {
		t.Fatalf("pending redirects: got %v", pending)
	}

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM search_fts WHERE entity_mbid IN ('8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1', '0a8e8d55-4b83-4d93-b31c-b7c9f8f5aeae')`).Scan(&count); err != nil {
		t.Fatalf("count fts rows: %v", err)
	}
	if count != 0 {
		t.Errorf("search_fts rows for pruned entities still present: %d", count)
	}

	var cursor string
	if err := db.QueryRowContext(ctx, `SELECT value FROM _meta WHERE key = ?`, mbdb.MetaKeyPruneReplicationSequence).Scan(&cursor); err != nil {
		t.Fatalf("read prune cursor: %v", err)
	}
	if cursor != "170432" {
		t.Errorf("prune cursor: got %q want %q", cursor, "170432")
	}

	if _, err := os.Stat(filepath.Join(cfg.DumpDir, "replication-170432-v2.tar.bz2")); !os.IsNotExist(err) {
		t.Errorf("packet file not cleaned up: %v", err)
	}

	if err := runPrune(ctx, cfg); err != nil {
		t.Fatalf("rerun runPrune: %v", err)
	}
}
