package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/zephyraoss/mbforge/internal/replication"
)

func applyEvents(t *testing.T, db *sql.DB, events *replication.Events, hasSearchIndex bool) PruneResult {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	result, err := ApplyPruneEvents(ctx, tx, events, hasSearchIndex, true)
	if err != nil {
		t.Fatalf("ApplyPruneEvents: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return result
}

func TestApplyPruneEventsDeletesEntitiesAndSearchRows(t *testing.T) {
	ctx := context.Background()
	db := newSearchIndexTestDB(t)
	if err := EnsurePruneSchema(ctx, db); err != nil {
		t.Fatalf("EnsurePruneSchema: %v", err)
	}
	if err := RebuildSearchIndex(ctx, db, SearchIndexOptions{IncludeTracks: true}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex: %v", err)
	}

	events := &replication.Events{
		Deletions: []replication.Deletion{
			{EntityType: "artist", MBID: "artist-1"},
			{EntityType: "label", MBID: "label-1"},
			{EntityType: "work", MBID: "work-1"},
			{EntityType: "release_group", MBID: "rg-1"},
			{EntityType: "release", MBID: "release-1"},
			{EntityType: "recording", MBID: "recording-1"},
			{EntityType: "artist", MBID: "artist-not-in-db"},
		},
		RowMBIDs: map[string]map[int64]string{},
	}
	result := applyEvents(t, db, events, true)

	want := map[string]int{"artist": 1, "label": 1, "work": 1, "release_group": 1, "release": 1, "recording": 1}
	for entityType, count := range want {
		if result.DeletedEntities[entityType] != count {
			t.Errorf("deleted %s: got %d want %d", entityType, result.DeletedEntities[entityType], count)
		}
	}

	for _, table := range []string{
		"artists", "artist_aliases", "labels", "label_aliases", "works", "work_aliases", "work_iswcs",
		"release_groups", "release_group_artists", "releases", "release_artists", "tracks",
		"recordings", "recording_artists",
	} {
		if got := countRows(t, db, `SELECT COUNT(*) FROM `+table); got != 0 {
			t.Errorf("%s: got %d rows, want 0", table, got)
		}
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM search_fts`); got != 0 {
		t.Errorf("search_fts: got %d rows, want 0", got)
	}
}

func TestApplyPruneEventsResolvesRedirectsFromHarvest(t *testing.T) {
	ctx := context.Background()
	db := newSearchIndexTestDB(t)
	if err := EnsurePruneSchema(ctx, db); err != nil {
		t.Fatalf("EnsurePruneSchema: %v", err)
	}

	events := &replication.Events{
		Deletions: []replication.Deletion{{EntityType: "recording", MBID: "recording-old"}},
		Redirects: []replication.Redirect{
			{EntityType: "recording", OldMBID: "recording-old", NewRowID: 78},
			{EntityType: "artist", OldMBID: "artist-merged", NewRowID: 999},
		},
		RowMBIDs: map[string]map[int64]string{
			"recording": {78: "recording-1"},
		},
	}
	result := applyEvents(t, db, events, false)

	if result.RedirectsApplied != 1 || result.RedirectsPending != 1 {
		t.Fatalf("redirects: applied=%d pending=%d, want 1/1", result.RedirectsApplied, result.RedirectsPending)
	}

	var newMBID string
	if err := db.QueryRowContext(ctx, `SELECT new_mbid FROM gid_redirects WHERE entity_type = 'recording' AND old_mbid = 'recording-old'`).Scan(&newMBID); err != nil {
		t.Fatalf("read redirect: %v", err)
	}
	if newMBID != "recording-1" {
		t.Errorf("redirect target: got %q want %q", newMBID, "recording-1")
	}

	pending, err := ListPendingRedirects(ctx, db, 10)
	if err != nil {
		t.Fatalf("ListPendingRedirects: %v", err)
	}
	if len(pending) != 1 || pending[0].EntityType != "artist" || pending[0].OldMBID != "artist-merged" || pending[0].NewRowID != 999 {
		t.Fatalf("pending: got %v", pending)
	}

	laterEvents := &replication.Events{
		RowMBIDs: map[string]map[int64]string{
			"artist": {999: "artist-1"},
		},
	}
	laterResult := applyEvents(t, db, laterEvents, false)
	if laterResult.PendingResolved != 1 {
		t.Fatalf("pending resolved: got %d want 1", laterResult.PendingResolved)
	}
	if err := db.QueryRowContext(ctx, `SELECT new_mbid FROM gid_redirects WHERE entity_type = 'artist' AND old_mbid = 'artist-merged'`).Scan(&newMBID); err != nil {
		t.Fatalf("read resolved redirect: %v", err)
	}
	if newMBID != "artist-1" {
		t.Errorf("resolved redirect target: got %q want %q", newMBID, "artist-1")
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM gid_redirects_pending`); got != 0 {
		t.Errorf("gid_redirects_pending: got %d rows, want 0", got)
	}
}

func TestApplyPruneEventsRepointsRedirectOnChainedMerge(t *testing.T) {
	ctx := context.Background()
	db := newSearchIndexTestDB(t)
	if err := EnsurePruneSchema(ctx, db); err != nil {
		t.Fatalf("EnsurePruneSchema: %v", err)
	}

	first := &replication.Events{
		Redirects: []replication.Redirect{{EntityType: "artist", OldMBID: "artist-a", NewRowID: 20}},
		RowMBIDs:  map[string]map[int64]string{"artist": {20: "artist-b"}},
	}
	applyEvents(t, db, first, false)

	second := &replication.Events{
		Redirects: []replication.Redirect{
			{EntityType: "artist", OldMBID: "artist-a", NewRowID: 30},
			{EntityType: "artist", OldMBID: "artist-b", NewRowID: 30},
		},
		RowMBIDs: map[string]map[int64]string{"artist": {30: "artist-c"}},
	}
	applyEvents(t, db, second, false)

	rows, err := db.QueryContext(ctx, `SELECT old_mbid, new_mbid FROM gid_redirects WHERE entity_type = 'artist' ORDER BY old_mbid`)
	if err != nil {
		t.Fatalf("read redirects: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var oldMBID, newMBID string
		if err := rows.Scan(&oldMBID, &newMBID); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[oldMBID] = newMBID
	}
	if got["artist-a"] != "artist-c" || got["artist-b"] != "artist-c" {
		t.Fatalf("chained redirects: got %v", got)
	}
}

func TestResolveAndDeletePendingRedirect(t *testing.T) {
	ctx := context.Background()
	db := newSearchIndexTestDB(t)
	if err := EnsurePruneSchema(ctx, db); err != nil {
		t.Fatalf("EnsurePruneSchema: %v", err)
	}

	events := &replication.Events{
		Redirects: []replication.Redirect{
			{EntityType: "release", OldMBID: "release-merged", NewRowID: 41},
			{EntityType: "work", OldMBID: "work-gone", NewRowID: 42},
		},
		RowMBIDs: map[string]map[int64]string{},
	}
	applyEvents(t, db, events, false)

	pending, err := ListPendingRedirects(ctx, db, 10)
	if err != nil {
		t.Fatalf("ListPendingRedirects: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending: got %d want 2", len(pending))
	}

	if err := ResolvePendingRedirect(ctx, db, pending[0], "release-1"); err != nil {
		t.Fatalf("ResolvePendingRedirect: %v", err)
	}
	var newMBID string
	if err := db.QueryRowContext(ctx, `SELECT new_mbid FROM gid_redirects WHERE entity_type = ? AND old_mbid = ?`, pending[0].EntityType, pending[0].OldMBID).Scan(&newMBID); err != nil {
		t.Fatalf("read redirect: %v", err)
	}
	if newMBID != "release-1" {
		t.Errorf("resolved target: got %q", newMBID)
	}

	if err := DeletePendingRedirect(ctx, db, pending[1]); err != nil {
		t.Fatalf("DeletePendingRedirect: %v", err)
	}
	if got, err := CountPendingRedirects(ctx, db); err != nil || got != 0 {
		t.Fatalf("pending after resolve+delete: got %d err %v", got, err)
	}
}
