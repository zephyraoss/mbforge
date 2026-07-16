package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/zephyraoss/mbforge/internal/libsqlutil"
	"github.com/zephyraoss/mbforge/internal/model"
)

func newUpsertTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := libsqlutil.OpenLocal(":memory:")
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)

	ctx := context.Background()
	if err := CreateSchema(ctx, db); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	return db
}

func applyUpsert(t *testing.T, db *sql.DB, m model.Mutation, entity string) map[string][]string {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	writer := NewUpsertWriter(ctx, tx)
	if err := writer.ApplyMutation(m, entity); err != nil {
		tx.Rollback()
		t.Fatalf("ApplyMutation(%s): %v", entity, err)
	}
	changed := writer.Changed()
	if err := writer.Close(); err != nil {
		tx.Rollback()
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return changed
}

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return count
}

func TestUpsertArtistReplacesChildren(t *testing.T) {
	db := newUpsertTestDB(t)

	first := model.Mutation{
		Artists: []model.ArtistRow{{MBID: "a1", Name: "Old Name", SortName: "Name, Old"}},
		ArtistAliases: []model.ArtistAliasRow{
			{ArtistMBID: "a1", Name: "Alias One"},
			{ArtistMBID: "a1", Name: "Alias Two"},
		},
		ArtistTags:    []model.ArtistTagRow{{ArtistMBID: "a1", Tag: "rock", Count: 3}},
		ExternalLinks: []model.ExternalLinkRow{{EntityType: "artist", EntityMBID: "a1", RelType: "official homepage", URL: "https://old.example"}},
	}
	applyUpsert(t, db, first, "artist")

	second := model.Mutation{
		Artists:       []model.ArtistRow{{MBID: "a1", Name: "New Name", SortName: "Name, New"}},
		ArtistAliases: []model.ArtistAliasRow{{ArtistMBID: "a1", Name: "Alias Three"}},
		ExternalLinks: []model.ExternalLinkRow{{EntityType: "artist", EntityMBID: "a1", RelType: "official homepage", URL: "https://new.example"}},
	}
	changed := applyUpsert(t, db, second, "artist")

	if got := countRows(t, db, `SELECT COUNT(*) FROM artists`); got != 1 {
		t.Fatalf("artists count: got %d want 1", got)
	}
	var name string
	if err := db.QueryRowContext(context.Background(), `SELECT name FROM artists WHERE mbid = 'a1'`).Scan(&name); err != nil {
		t.Fatalf("select artist: %v", err)
	}
	if name != "New Name" {
		t.Fatalf("artist name: got %q want %q", name, "New Name")
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM artist_aliases WHERE artist_mbid = 'a1'`); got != 1 {
		t.Fatalf("alias count: got %d want 1", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM artist_tags WHERE artist_mbid = 'a1'`); got != 0 {
		t.Fatalf("tag count: got %d want 0", got)
	}
	var url string
	if err := db.QueryRowContext(context.Background(), `SELECT url FROM external_links WHERE entity_type = 'artist' AND entity_mbid = 'a1'`).Scan(&url); err != nil {
		t.Fatalf("select external link: %v", err)
	}
	if url != "https://new.example" {
		t.Fatalf("external link: got %q want %q", url, "https://new.example")
	}
	if got := changed["artist"]; len(got) != 1 || got[0] != "a1" {
		t.Fatalf("changed artists: got %v want [a1]", got)
	}
}

func TestUpsertReleaseReplacesTracksAndKeepsExistingRecordings(t *testing.T) {
	db := newUpsertTestDB(t)

	length := 100
	applyUpsert(t, db, model.Mutation{
		Recordings:     []model.RecordingRow{{MBID: "rec1", Title: "Full Title", Length: &length}},
		RecordingISRCs: []model.RecordingISRCRow{{RecordingMBID: "rec1", ISRC: "USRC17607839"}},
	}, "recording")

	first := model.Mutation{
		Releases: []model.ReleaseRow{{MBID: "rel1", Title: "Old Release"}},
		Tracks: []model.TrackRow{
			{MBID: "t1", ReleaseMBID: "rel1", RecordingMBID: "rec1", MediaPosition: 1, Position: 1, Number: "1", Title: "Old Track"},
			{MBID: "t2", ReleaseMBID: "rel1", RecordingMBID: "rec1", MediaPosition: 1, Position: 2, Number: "2", Title: "Dropped Track"},
		},
		Recordings: []model.RecordingRow{{MBID: "rec1", Title: "Embedded Title"}},
	}
	applyUpsert(t, db, first, "release")

	var title string
	if err := db.QueryRowContext(context.Background(), `SELECT title FROM recordings WHERE mbid = 'rec1'`).Scan(&title); err != nil {
		t.Fatalf("select recording: %v", err)
	}
	if title != "Full Title" {
		t.Fatalf("recording title: got %q want %q", title, "Full Title")
	}

	second := model.Mutation{
		Releases: []model.ReleaseRow{{MBID: "rel1", Title: "New Release"}},
		Tracks: []model.TrackRow{
			{MBID: "t1", ReleaseMBID: "rel1", RecordingMBID: "rec2", MediaPosition: 1, Position: 1, Number: "1", Title: "New Track"},
		},
		Recordings: []model.RecordingRow{{MBID: "rec2", Title: "Brand New"}},
		RecordingISRCs: []model.RecordingISRCRow{
			{RecordingMBID: "rec1", ISRC: "MUSTNOTAPPEAR"},
			{RecordingMBID: "rec2", ISRC: "GBUM71029604"},
		},
	}
	changed := applyUpsert(t, db, second, "release")

	if got := countRows(t, db, `SELECT COUNT(*) FROM tracks WHERE release_mbid = 'rel1'`); got != 1 {
		t.Fatalf("track count: got %d want 1", got)
	}
	found := false
	for _, mbid := range changed["track"] {
		if mbid == "t2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("changed tracks missing dropped track t2: %v", changed["track"])
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM recordings`); got != 2 {
		t.Fatalf("recording count: got %d want 2", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_isrcs WHERE recording_mbid = 'rec1'`); got != 1 {
		t.Fatalf("rec1 isrc count: got %d want 1", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_isrcs WHERE recording_mbid = 'rec2'`); got != 1 {
		t.Fatalf("rec2 isrc count: got %d want 1", got)
	}
}

func TestUpsertRecordingReplacesChildren(t *testing.T) {
	db := newUpsertTestDB(t)

	applyUpsert(t, db, model.Mutation{
		Recordings:     []model.RecordingRow{{MBID: "rec1", Title: "Old"}},
		RecordingISRCs: []model.RecordingISRCRow{{RecordingMBID: "rec1", ISRC: "USRC17607839"}},
		RecordingTags:  []model.RecordingTagRow{{RecordingMBID: "rec1", Tag: "jazz", Count: 1}},
	}, "recording")

	applyUpsert(t, db, model.Mutation{
		Recordings:     []model.RecordingRow{{MBID: "rec1", Title: "New"}},
		RecordingISRCs: []model.RecordingISRCRow{{RecordingMBID: "rec1", ISRC: "GBUM71029604"}},
	}, "recording")

	var title string
	if err := db.QueryRowContext(context.Background(), `SELECT title FROM recordings WHERE mbid = 'rec1'`).Scan(&title); err != nil {
		t.Fatalf("select recording: %v", err)
	}
	if title != "New" {
		t.Fatalf("recording title: got %q want %q", title, "New")
	}
	var isrc string
	if err := db.QueryRowContext(context.Background(), `SELECT isrc FROM recording_isrcs WHERE recording_mbid = 'rec1'`).Scan(&isrc); err != nil {
		t.Fatalf("select isrc: %v", err)
	}
	if isrc != "GBUM71029604" {
		t.Fatalf("isrc: got %q want %q", isrc, "GBUM71029604")
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_tags WHERE recording_mbid = 'rec1'`); got != 0 {
		t.Fatalf("tag count: got %d want 0", got)
	}
}

func TestUpsertArtistReplacesRelationships(t *testing.T) {
	db := newUpsertTestDB(t)

	applyUpsert(t, db, model.Mutation{
		Artists: []model.ArtistRow{{MBID: "a1", Name: "Member", SortName: "Member"}},
		ArtistRelationships: []model.ArtistRelationshipRow{
			{ArtistMBID: "a1", RelatedArtistMBID: "band-1", RelatedArtistName: "Old Band", Type: "member of band", Direction: "backward", BeginDate: "1990", EndDate: "1995", Ended: true},
			{ArtistMBID: "a1", RelatedArtistMBID: "band-2", Type: "collaboration", Direction: "forward"},
		},
	}, "artist")

	applyUpsert(t, db, model.Mutation{
		Artists: []model.ArtistRow{{MBID: "a2", Name: "Other", SortName: "Other"}},
		ArtistRelationships: []model.ArtistRelationshipRow{
			{ArtistMBID: "a2", RelatedArtistMBID: "a1", Type: "teacher", Direction: "forward"},
		},
	}, "artist")

	applyUpsert(t, db, model.Mutation{
		Artists: []model.ArtistRow{{MBID: "a1", Name: "Member", SortName: "Member"}},
		ArtistRelationships: []model.ArtistRelationshipRow{
			{ArtistMBID: "a1", RelatedArtistMBID: "band-1", RelatedArtistName: "New Band", Type: "member of band", Direction: "backward", BeginDate: "1990", EndDate: "1995", Ended: true, Attributes: `["guitar"]`},
		},
	}, "artist")

	if got := countRows(t, db, `SELECT COUNT(*) FROM artist_relationships WHERE artist_mbid = 'a1'`); got != 1 {
		t.Fatalf("a1 relationship count: got %d want 1", got)
	}
	var attributes string
	if err := db.QueryRowContext(context.Background(), `SELECT attributes FROM artist_relationships WHERE artist_mbid = 'a1'`).Scan(&attributes); err != nil {
		t.Fatalf("select relationship: %v", err)
	}
	if attributes != `["guitar"]` {
		t.Fatalf("relationship attributes: got %q want %q", attributes, `["guitar"]`)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM artist_relationships WHERE artist_mbid = 'a2'`); got != 1 {
		t.Fatalf("a2 relationship count: got %d want 1", got)
	}
}

func TestUpsertLabelReplacesChildren(t *testing.T) {
	db := newUpsertTestDB(t)

	code := 10298
	applyUpsert(t, db, model.Mutation{
		Labels:        []model.LabelRow{{MBID: "l1", Name: "Old Label", SortName: "Old Label", LabelCode: &code}},
		LabelAliases:  []model.LabelAliasRow{{LabelMBID: "l1", Name: "Alias One"}},
		LabelTags:     []model.LabelTagRow{{LabelMBID: "l1", Tag: "classical", Count: 2}},
		LabelGenres:   []model.LabelGenreRow{{LabelMBID: "l1", GenreMBID: "g1", GenreName: "classical", Count: 2}},
		ExternalLinks: []model.ExternalLinkRow{{EntityType: "label", EntityMBID: "l1", RelType: "official site", URL: "https://old.example"}},
	}, "label")

	changed := applyUpsert(t, db, model.Mutation{
		Labels:        []model.LabelRow{{MBID: "l1", Name: "New Label", SortName: "New Label"}},
		LabelAliases:  []model.LabelAliasRow{{LabelMBID: "l1", Name: "Alias Two"}},
		ExternalLinks: []model.ExternalLinkRow{{EntityType: "label", EntityMBID: "l1", RelType: "official site", URL: "https://new.example"}},
	}, "label")

	var name string
	var labelCode any
	if err := db.QueryRowContext(context.Background(), `SELECT name, label_code FROM labels WHERE mbid = 'l1'`).Scan(&name, &labelCode); err != nil {
		t.Fatalf("select label: %v", err)
	}
	if name != "New Label" {
		t.Fatalf("label name: got %q want %q", name, "New Label")
	}
	if labelCode != nil {
		t.Fatalf("label code: got %v want NULL", labelCode)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM label_aliases WHERE label_mbid = 'l1'`); got != 1 {
		t.Fatalf("alias count: got %d want 1", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM label_tags WHERE label_mbid = 'l1'`); got != 0 {
		t.Fatalf("tag count: got %d want 0", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM label_genres WHERE label_mbid = 'l1'`); got != 0 {
		t.Fatalf("genre count: got %d want 0", got)
	}
	var url string
	if err := db.QueryRowContext(context.Background(), `SELECT url FROM external_links WHERE entity_type = 'label' AND entity_mbid = 'l1'`).Scan(&url); err != nil {
		t.Fatalf("select external link: %v", err)
	}
	if url != "https://new.example" {
		t.Fatalf("external link: got %q want %q", url, "https://new.example")
	}
	if got := changed["label"]; len(got) != 1 || got[0] != "l1" {
		t.Fatalf("changed labels: got %v want [l1]", got)
	}
}

func TestUpsertWorkReplacesChildrenAndRecordingLinks(t *testing.T) {
	db := newUpsertTestDB(t)

	applyUpsert(t, db, model.Mutation{
		Works:     []model.WorkRow{{MBID: "w1", Title: "Old Title", Languages: "eng"}},
		WorkISWCs: []model.WorkISWCRow{{WorkMBID: "w1", ISWC: "T-000.000.001-0"}},
		WorkTags:  []model.WorkTagRow{{WorkMBID: "w1", Tag: "song", Count: 1}},
		RecordingWorks: []model.RecordingWorkRow{
			{RecordingMBID: "rec1", WorkMBID: "w1", Type: "performance"},
			{RecordingMBID: "rec2", WorkMBID: "w1", Type: "performance", Attributes: `["cover"]`},
		},
	}, "work")

	changed := applyUpsert(t, db, model.Mutation{
		Works:     []model.WorkRow{{MBID: "w1", Title: "New Title", Languages: "eng"}},
		WorkISWCs: []model.WorkISWCRow{{WorkMBID: "w1", ISWC: "T-000.000.002-0"}},
		RecordingWorks: []model.RecordingWorkRow{
			{RecordingMBID: "rec3", WorkMBID: "w1", Type: "performance"},
		},
	}, "work")

	var title string
	if err := db.QueryRowContext(context.Background(), `SELECT title FROM works WHERE mbid = 'w1'`).Scan(&title); err != nil {
		t.Fatalf("select work: %v", err)
	}
	if title != "New Title" {
		t.Fatalf("work title: got %q want %q", title, "New Title")
	}
	var iswc string
	if err := db.QueryRowContext(context.Background(), `SELECT iswc FROM work_iswcs WHERE work_mbid = 'w1'`).Scan(&iswc); err != nil {
		t.Fatalf("select iswc: %v", err)
	}
	if iswc != "T-000.000.002-0" {
		t.Fatalf("iswc: got %q want %q", iswc, "T-000.000.002-0")
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM work_tags WHERE work_mbid = 'w1'`); got != 0 {
		t.Fatalf("tag count: got %d want 0", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_works WHERE work_mbid = 'w1'`); got != 1 {
		t.Fatalf("recording link count: got %d want 1", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_works WHERE recording_mbid = 'rec3'`); got != 1 {
		t.Fatalf("rec3 link count: got %d want 1", got)
	}
	if got := changed["work"]; len(got) != 1 || got[0] != "w1" {
		t.Fatalf("changed works: got %v want [w1]", got)
	}
}

func TestUpsertRecordingReplacesWorkLinksAndKeepsExistingWorks(t *testing.T) {
	db := newUpsertTestDB(t)

	applyUpsert(t, db, model.Mutation{
		Works:     []model.WorkRow{{MBID: "w1", Title: "Full Work Title", Languages: "eng"}},
		WorkISWCs: []model.WorkISWCRow{{WorkMBID: "w1", ISWC: "T-000.000.001-0"}},
	}, "work")

	applyUpsert(t, db, model.Mutation{
		Recordings: []model.RecordingRow{{MBID: "rec1", Title: "Recording"}},
		Works:      []model.WorkRow{{MBID: "w1", Title: "Embedded Title"}},
		RecordingWorks: []model.RecordingWorkRow{
			{RecordingMBID: "rec1", WorkMBID: "w1", Type: "performance", Attributes: `["live"]`},
		},
	}, "recording")

	var title string
	if err := db.QueryRowContext(context.Background(), `SELECT title FROM works WHERE mbid = 'w1'`).Scan(&title); err != nil {
		t.Fatalf("select work: %v", err)
	}
	if title != "Full Work Title" {
		t.Fatalf("work title: got %q want %q", title, "Full Work Title")
	}

	changed := applyUpsert(t, db, model.Mutation{
		Recordings: []model.RecordingRow{{MBID: "rec1", Title: "Recording"}},
		Works: []model.WorkRow{
			{MBID: "w2", Title: "Brand New Work"},
		},
		WorkISWCs: []model.WorkISWCRow{
			{WorkMBID: "w1", ISWC: "MUSTNOTAPPEAR"},
			{WorkMBID: "w2", ISWC: "T-000.000.002-0"},
		},
		RecordingWorks: []model.RecordingWorkRow{
			{RecordingMBID: "rec1", WorkMBID: "w2", Type: "performance"},
		},
	}, "recording")

	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_works WHERE recording_mbid = 'rec1'`); got != 1 {
		t.Fatalf("recording link count: got %d want 1", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_works WHERE recording_mbid = 'rec1' AND work_mbid = 'w2'`); got != 1 {
		t.Fatalf("w2 link count: got %d want 1", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM works`); got != 2 {
		t.Fatalf("work count: got %d want 2", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM work_iswcs WHERE work_mbid = 'w1'`); got != 1 {
		t.Fatalf("w1 iswc count: got %d want 1", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM work_iswcs WHERE work_mbid = 'w2'`); got != 1 {
		t.Fatalf("w2 iswc count: got %d want 1", got)
	}
	found := false
	for _, mbid := range changed["work"] {
		if mbid == "w2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("changed works missing w2: %v", changed["work"])
	}
}

func TestUpsertReleaseOnlyLinksWorksOfNewRecordings(t *testing.T) {
	db := newUpsertTestDB(t)

	applyUpsert(t, db, model.Mutation{
		Recordings: []model.RecordingRow{{MBID: "rec1", Title: "Existing"}},
	}, "recording")

	applyUpsert(t, db, model.Mutation{
		Releases: []model.ReleaseRow{{MBID: "rel1", Title: "Release"}},
		Tracks: []model.TrackRow{
			{MBID: "t1", ReleaseMBID: "rel1", RecordingMBID: "rec1", MediaPosition: 1, Position: 1, Number: "1", Title: "One"},
			{MBID: "t2", ReleaseMBID: "rel1", RecordingMBID: "rec2", MediaPosition: 1, Position: 2, Number: "2", Title: "Two"},
		},
		Recordings: []model.RecordingRow{
			{MBID: "rec1", Title: "Existing"},
			{MBID: "rec2", Title: "Embedded New"},
		},
		Works: []model.WorkRow{{MBID: "w1", Title: "Work"}},
		RecordingWorks: []model.RecordingWorkRow{
			{RecordingMBID: "rec1", WorkMBID: "w1", Type: "performance"},
			{RecordingMBID: "rec2", WorkMBID: "w1", Type: "performance"},
		},
	}, "release")

	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_works WHERE recording_mbid = 'rec1'`); got != 0 {
		t.Fatalf("rec1 link count: got %d want 0", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM recording_works WHERE recording_mbid = 'rec2'`); got != 1 {
		t.Fatalf("rec2 link count: got %d want 1", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM works`); got != 1 {
		t.Fatalf("work count: got %d want 1", got)
	}
}

func TestRefreshSearchIndexRowsUpdatesAndRemoves(t *testing.T) {
	db := newUpsertTestDB(t)
	ctx := context.Background()

	applyUpsert(t, db, model.Mutation{
		Artists: []model.ArtistRow{{MBID: "a1", Name: "Original Artist", SortName: "Artist, Original"}},
	}, "artist")
	if err := RebuildSearchIndex(ctx, db, SearchIndexOptions{IncludeTracks: true}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex: %v", err)
	}

	changed := applyUpsert(t, db, model.Mutation{
		Artists: []model.ArtistRow{{MBID: "a1", Name: "Renamed Artist", SortName: "Artist, Renamed"}},
	}, "artist")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO search_fts(entity_type, entity_mbid, heading, subtitle, meta, aux) VALUES('artist', 'gone', 'Stale', '', '', '')`); err != nil {
		t.Fatalf("insert stale row: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO search_fts_map(entity_type, entity_mbid, fts_rowid) VALUES('artist', 'gone', last_insert_rowid())`); err != nil {
		t.Fatalf("map stale row: %v", err)
	}
	changed["artist"] = append(changed["artist"], "gone")
	if err := RefreshSearchIndexRows(ctx, tx, changed, true); err != nil {
		t.Fatalf("RefreshSearchIndexRows: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var heading string
	if err := db.QueryRowContext(ctx, `SELECT heading FROM search_fts WHERE entity_type = 'artist' AND entity_mbid = 'a1'`).Scan(&heading); err != nil {
		t.Fatalf("select fts row: %v", err)
	}
	if heading != "Renamed Artist" {
		t.Fatalf("fts heading: got %q want %q", heading, "Renamed Artist")
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM search_fts WHERE entity_mbid = 'gone'`); got != 0 {
		t.Fatalf("stale fts rows: got %d want 0", got)
	}
}

func TestRefreshSearchIndexRowsHonorsTrackExclusion(t *testing.T) {
	db := newUpsertTestDB(t)
	ctx := context.Background()

	changed := applyUpsert(t, db, model.Mutation{
		Releases:   []model.ReleaseRow{{MBID: "rel1", Title: "Release"}},
		Recordings: []model.RecordingRow{{MBID: "rec1", Title: "Song"}},
		Tracks:     []model.TrackRow{{MBID: "t1", ReleaseMBID: "rel1", RecordingMBID: "rec1", MediaPosition: 1, Position: 1, Number: "1", Title: "Song"}},
	}, "release")
	if len(changed["track"]) == 0 {
		t.Fatalf("expected changed tracks, got %v", changed)
	}

	if err := RebuildSearchIndex(ctx, db, SearchIndexOptions{IncludeTracks: false}, nil); err != nil {
		t.Fatalf("RebuildSearchIndex: %v", err)
	}

	refresh := func(includeTracks bool) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if err := RefreshSearchIndexRows(ctx, tx, changed, includeTracks); err != nil {
			t.Fatalf("RefreshSearchIndexRows: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	refresh(false)
	if got := countRows(t, db, `SELECT COUNT(*) FROM search_fts WHERE entity_type = 'track'`); got != 0 {
		t.Fatalf("track fts rows after excluded refresh: got %d want 0", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM search_fts WHERE entity_type = 'release' AND entity_mbid = 'rel1'`); got != 1 {
		t.Fatalf("release fts rows after excluded refresh: got %d want 1", got)
	}

	refresh(true)
	if got := countRows(t, db, `SELECT COUNT(*) FROM search_fts WHERE entity_type = 'track' AND entity_mbid = 't1'`); got != 1 {
		t.Fatalf("track fts rows after included refresh: got %d want 1", got)
	}
}
