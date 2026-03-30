package db

import (
	"context"
	"database/sql"
	"fmt"
)

const createSearchFTSSQL = `
CREATE VIRTUAL TABLE search_fts USING fts5(
    entity_type UNINDEXED,
    entity_mbid UNINDEXED,
    heading,
    subtitle,
    meta,
    aux,
    tokenize = 'unicode61 remove_diacritics 2'
);`

var searchIndexPopulateStages = []struct {
	name string
	sql  string
}{
	{
		name: "artists",
		sql: `
INSERT INTO search_fts(entity_type, entity_mbid, heading, subtitle, meta, aux)
SELECT
    'artist',
    a.mbid,
    a.name,
    COALESCE(a.sort_name, ''),
    trim(COALESCE(a.type, '') || CASE WHEN COALESCE(a.type, '') <> '' AND COALESCE(a.country, '') <> '' THEN ' ' ELSE '' END || COALESCE(a.country, '')),
    COALESCE((
        SELECT group_concat(piece, ' ')
        FROM (
            SELECT aa.name AS piece
            FROM artist_aliases aa
            WHERE aa.artist_mbid = a.mbid
            ORDER BY aa.is_primary DESC, aa.name
        )
    ), '')
FROM artists a;`,
	},
	{
		name: "release_groups",
		sql: `
INSERT INTO search_fts(entity_type, entity_mbid, heading, subtitle, meta, aux)
SELECT
    'release_group',
    rg.mbid,
    rg.title,
    COALESCE((
        SELECT group_concat(piece, '')
        FROM (
            SELECT rga.artist_name || rga.join_phrase AS piece
            FROM release_group_artists rga
            WHERE rga.release_group_mbid = rg.mbid
            ORDER BY rga.position
        )
    ), ''),
    trim(COALESCE(rg.primary_type, '') || CASE WHEN COALESCE(rg.primary_type, '') <> '' AND COALESCE(rg.first_release_date, '') <> '' THEN ' ' ELSE '' END || COALESCE(rg.first_release_date, '')),
    COALESCE(rg.disambiguation, '')
FROM release_groups rg;`,
	},
	{
		name: "releases",
		sql: `
INSERT INTO search_fts(entity_type, entity_mbid, heading, subtitle, meta, aux)
SELECT
    'release',
    r.mbid,
    r.title,
    COALESCE((
        SELECT group_concat(piece, '')
        FROM (
            SELECT ra.artist_name || ra.join_phrase AS piece
            FROM release_artists ra
            WHERE ra.release_mbid = r.mbid
            ORDER BY ra.position
        )
    ), ''),
    trim(COALESCE(r.date, '') || CASE WHEN COALESCE(r.date, '') <> '' AND COALESCE(r.country, '') <> '' THEN ' ' ELSE '' END || COALESCE(r.country, '')),
    trim(COALESCE(r.barcode, '') || CASE WHEN COALESCE(r.barcode, '') <> '' THEN ' ' ELSE '' END || COALESCE((
        SELECT group_concat(piece, ' ')
        FROM (
            SELECT rl.label_name AS piece
            FROM release_labels rl
            WHERE rl.release_mbid = r.mbid AND rl.label_name <> ''
            ORDER BY rl.label_name
        )
    ), ''))
FROM releases r;`,
	},
	{
		name: "recordings",
		sql: `
INSERT INTO search_fts(entity_type, entity_mbid, heading, subtitle, meta, aux)
SELECT
    'recording',
    r.mbid,
    r.title,
    COALESCE((
        SELECT group_concat(piece, '')
        FROM (
            SELECT ra.artist_name || ra.join_phrase AS piece
            FROM recording_artists ra
            WHERE ra.recording_mbid = r.mbid
            ORDER BY ra.position
        )
    ), ''),
    COALESCE(r.first_release_date, ''),
    COALESCE((
        SELECT group_concat(piece, ' ')
        FROM (
            SELECT ri.isrc AS piece
            FROM recording_isrcs ri
            WHERE ri.recording_mbid = r.mbid
            ORDER BY ri.isrc
        )
    ), '')
FROM recordings r;`,
	},
	{
		name: "tracks",
		sql: `
INSERT INTO search_fts(entity_type, entity_mbid, heading, subtitle, meta, aux)
SELECT
    'track',
    t.mbid,
    t.title,
    COALESCE(r.title, ''),
    COALESCE((
        SELECT group_concat(piece, '')
        FROM (
            SELECT ra.artist_name || ra.join_phrase AS piece
            FROM release_artists ra
            WHERE ra.release_mbid = t.release_mbid
            ORDER BY ra.position
        )
    ), ''),
    COALESCE(t.number, '')
FROM tracks t
JOIN releases r ON r.mbid = t.release_mbid;`,
	},
}

func SearchIndexExists(ctx context.Context, db *sql.DB) (bool, error) {
	var found string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'search_fts'`).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return found == "search_fts", nil
}

func RebuildSearchIndex(ctx context.Context, db *sql.DB, logf func(string, ...any)) error {
	if logf != nil {
		logf("rebuilding search index")
	}

	for _, stmt := range []string{
		`DROP TABLE IF EXISTS search_fts;`,
		createSearchFTSSQL,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	for _, stage := range searchIndexPopulateStages {
		if logf != nil {
			logf("search index stage=%s", stage.name)
		}
		if _, err := db.ExecContext(ctx, stage.sql); err != nil {
			return fmt.Errorf("populate search index %s: %w", stage.name, err)
		}
	}

	if logf != nil {
		logf("search index optimize")
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO search_fts(search_fts) VALUES('optimize');`); err != nil {
		return fmt.Errorf("optimize search index: %w", err)
	}
	return nil
}
