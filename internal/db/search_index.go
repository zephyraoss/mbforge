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

const createSearchMapSQL = `
CREATE TABLE IF NOT EXISTS search_fts_map (
    entity_type TEXT NOT NULL,
    entity_mbid TEXT NOT NULL,
    fts_rowid INTEGER NOT NULL
);`

const createSearchMapIndexSQL = `CREATE INDEX IF NOT EXISTS idx_search_fts_map_entity ON search_fts_map(entity_type, entity_mbid);`

const populateSearchMapSQL = `INSERT INTO search_fts_map(entity_type, entity_mbid, fts_rowid) SELECT entity_type, entity_mbid, rowid FROM search_fts;`

var searchIndexPopulateStages = []struct {
	name       string
	entityType string
	sql        string
	rowFilter  string
}{
	{
		name:       "artists",
		entityType: "artist",
		rowFilter:  "WHERE a.mbid = ?",
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
FROM artists a`,
	},
	{
		name:       "labels",
		entityType: "label",
		rowFilter:  "WHERE l.mbid = ?",
		sql: `
INSERT INTO search_fts(entity_type, entity_mbid, heading, subtitle, meta, aux)
SELECT
    'label',
    l.mbid,
    l.name,
    COALESCE(l.sort_name, ''),
    trim(COALESCE(l.type, '') || CASE WHEN COALESCE(l.type, '') <> '' AND COALESCE(l.country, '') <> '' THEN ' ' ELSE '' END || COALESCE(l.country, '')),
    COALESCE((
        SELECT group_concat(piece, ' ')
        FROM (
            SELECT la.name AS piece
            FROM label_aliases la
            WHERE la.label_mbid = l.mbid
            ORDER BY la.is_primary DESC, la.name
        )
    ), '')
FROM labels l`,
	},
	{
		name:       "works",
		entityType: "work",
		rowFilter:  "WHERE w.mbid = ?",
		sql: `
INSERT INTO search_fts(entity_type, entity_mbid, heading, subtitle, meta, aux)
SELECT
    'work',
    w.mbid,
    w.title,
    COALESCE(w.disambiguation, ''),
    trim(COALESCE(w.type, '') || CASE WHEN COALESCE(w.type, '') <> '' AND COALESCE(w.languages, '') <> '' THEN ' ' ELSE '' END || COALESCE(w.languages, '')),
    trim(COALESCE((
        SELECT group_concat(piece, ' ')
        FROM (
            SELECT wa.name AS piece
            FROM work_aliases wa
            WHERE wa.work_mbid = w.mbid
            ORDER BY wa.is_primary DESC, wa.name
        )
    ), '') || ' ' || COALESCE((
        SELECT group_concat(piece, ' ')
        FROM (
            SELECT wi.iswc AS piece
            FROM work_iswcs wi
            WHERE wi.work_mbid = w.mbid
            ORDER BY wi.iswc
        )
    ), ''))
FROM works w`,
	},
	{
		name:       "release_groups",
		entityType: "release_group",
		rowFilter:  "WHERE rg.mbid = ?",
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
FROM release_groups rg`,
	},
	{
		name:       "releases",
		entityType: "release",
		rowFilter:  "WHERE r.mbid = ?",
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
FROM releases r`,
	},
	{
		name:       "recordings",
		entityType: "recording",
		rowFilter:  "WHERE r.mbid = ?",
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
FROM recordings r`,
	},
	{
		name:       "tracks",
		entityType: "track",
		rowFilter:  "WHERE t.mbid = ?",
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
JOIN releases r ON r.mbid = t.release_mbid`,
	},
}

const MetaKeySearchIndexTracks = "search_index_tracks"

type SearchIndexOptions struct {
	IncludeTracks bool
}

func SearchIndexIncludesTracks(meta map[string]string) bool {
	return meta[MetaKeySearchIndexTracks] != "false"
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

func RebuildSearchIndex(ctx context.Context, db *sql.DB, opts SearchIndexOptions, logf func(string, ...any)) error {
	if logf != nil {
		logf("rebuilding search index include_tracks=%t", opts.IncludeTracks)
	}

	for _, stmt := range []string{
		`DROP TABLE IF EXISTS search_fts;`,
		`DROP TABLE IF EXISTS search_fts_map;`,
		createSearchFTSSQL,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	for _, stage := range searchIndexPopulateStages {
		if stage.entityType == "track" && !opts.IncludeTracks {
			if logf != nil {
				logf("search index stage=%s skipped", stage.name)
			}
			continue
		}
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

	if logf != nil {
		logf("search index row map")
	}
	for _, stmt := range []string{createSearchMapSQL, populateSearchMapSQL, createSearchMapIndexSQL} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("build search index row map: %w", err)
		}
	}

	if _, err := db.ExecContext(ctx, createMetaTable); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO _meta(key, value) VALUES(?, ?)`,
		MetaKeySearchIndexTracks, fmt.Sprintf("%t", opts.IncludeTracks)); err != nil {
		return fmt.Errorf("record search index options: %w", err)
	}
	return nil
}

func EnsureSearchIndexMap(ctx context.Context, db *sql.DB, logf func(string, ...any)) error {
	var found string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'search_fts_map'`).Scan(&found)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}

	if logf != nil {
		logf("building search index row map (one-time migration, scans the whole search index)")
	}

	var tempStore int
	if err := db.QueryRowContext(ctx, `PRAGMA temp_store`).Scan(&tempStore); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA temp_store = FILE`); err != nil {
		return err
	}
	defer db.ExecContext(ctx, fmt.Sprintf(`PRAGMA temp_store = %d`, tempStore))

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, stmt := range []string{createSearchMapSQL, populateSearchMapSQL, createSearchMapIndexSQL} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("build search index row map: %w", err)
		}
	}
	return tx.Commit()
}

func RefreshSearchIndexRows(ctx context.Context, tx *sql.Tx, changed map[string][]string, includeTracks bool) error {
	deleteFTSStmt, err := tx.PrepareContext(ctx, `DELETE FROM search_fts WHERE rowid IN (SELECT fts_rowid FROM search_fts_map WHERE entity_type = ? AND entity_mbid = ?)`)
	if err != nil {
		return err
	}
	defer deleteFTSStmt.Close()
	deleteMapStmt, err := tx.PrepareContext(ctx, `DELETE FROM search_fts_map WHERE entity_type = ? AND entity_mbid = ?`)
	if err != nil {
		return err
	}
	defer deleteMapStmt.Close()
	insertMapStmt, err := tx.PrepareContext(ctx, `INSERT INTO search_fts_map(entity_type, entity_mbid, fts_rowid) SELECT ?, ?, last_insert_rowid() WHERE changes() > 0`)
	if err != nil {
		return err
	}
	defer insertMapStmt.Close()

	for _, stage := range searchIndexPopulateStages {
		if stage.entityType == "track" && !includeTracks {
			continue
		}
		mbids := changed[stage.entityType]
		if len(mbids) == 0 {
			continue
		}
		insertStmt, err := tx.PrepareContext(ctx, stage.sql+"\n"+stage.rowFilter)
		if err != nil {
			return fmt.Errorf("refresh search index %s: %w", stage.name, err)
		}
		for _, mbid := range mbids {
			if _, err := deleteFTSStmt.ExecContext(ctx, stage.entityType, mbid); err != nil {
				insertStmt.Close()
				return fmt.Errorf("refresh search index %s: %w", stage.name, err)
			}
			if _, err := deleteMapStmt.ExecContext(ctx, stage.entityType, mbid); err != nil {
				insertStmt.Close()
				return fmt.Errorf("refresh search index %s: %w", stage.name, err)
			}
			if _, err := insertStmt.ExecContext(ctx, mbid); err != nil {
				insertStmt.Close()
				return fmt.Errorf("refresh search index %s: %w", stage.name, err)
			}
			if _, err := insertMapStmt.ExecContext(ctx, stage.entityType, mbid); err != nil {
				insertStmt.Close()
				return fmt.Errorf("refresh search index %s: %w", stage.name, err)
			}
		}
		insertStmt.Close()
	}
	return nil
}
