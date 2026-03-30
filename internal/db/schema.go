package db

import (
	"context"
	"database/sql"
	"fmt"
)

const createMetaTable = `
CREATE TABLE IF NOT EXISTS _meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);`

var createTables = []string{
	`
CREATE TABLE IF NOT EXISTS artists (
    mbid           TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    sort_name      TEXT NOT NULL,
    disambiguation TEXT NOT NULL DEFAULT '',
    type           TEXT,
    country        TEXT,
    gender         TEXT,
    begin_date     TEXT,
    end_date       TEXT,
    ended          INTEGER NOT NULL DEFAULT 0,
    area_mbid      TEXT,
    area_name      TEXT
);`,
	`
CREATE TABLE IF NOT EXISTS artist_aliases (
    artist_mbid TEXT NOT NULL REFERENCES artists(mbid),
    name        TEXT NOT NULL,
    sort_name   TEXT,
    type        TEXT,
    locale      TEXT NOT NULL DEFAULT '',
    is_primary  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (artist_mbid, name, locale)
);`,
	`
CREATE TABLE IF NOT EXISTS artist_tags (
    artist_mbid TEXT NOT NULL REFERENCES artists(mbid),
    tag         TEXT NOT NULL,
    count       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (artist_mbid, tag)
);`,
	`
CREATE TABLE IF NOT EXISTS artist_genres (
    artist_mbid TEXT NOT NULL REFERENCES artists(mbid),
    genre_mbid  TEXT NOT NULL,
    genre_name  TEXT NOT NULL,
    count       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (artist_mbid, genre_mbid)
);`,
	`
CREATE TABLE IF NOT EXISTS release_groups (
    mbid               TEXT PRIMARY KEY,
    title              TEXT NOT NULL,
    primary_type       TEXT,
    disambiguation     TEXT NOT NULL DEFAULT '',
    first_release_date TEXT
);`,
	`
CREATE TABLE IF NOT EXISTS release_group_secondary_types (
    release_group_mbid TEXT NOT NULL REFERENCES release_groups(mbid),
    type               TEXT NOT NULL,
    PRIMARY KEY (release_group_mbid, type)
);`,
	`
CREATE TABLE IF NOT EXISTS release_group_artists (
    release_group_mbid TEXT NOT NULL REFERENCES release_groups(mbid),
    artist_mbid        TEXT NOT NULL,
    artist_name        TEXT NOT NULL,
    join_phrase        TEXT NOT NULL DEFAULT '',
    position           INTEGER NOT NULL,
    PRIMARY KEY (release_group_mbid, position)
);`,
	`
CREATE TABLE IF NOT EXISTS release_group_tags (
    release_group_mbid TEXT NOT NULL REFERENCES release_groups(mbid),
    tag                TEXT NOT NULL,
    count              INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (release_group_mbid, tag)
);`,
	`
CREATE TABLE IF NOT EXISTS releases (
    mbid               TEXT PRIMARY KEY,
    title              TEXT NOT NULL,
    status             TEXT,
    date               TEXT,
    country            TEXT,
    barcode            TEXT,
    packaging          TEXT,
    language           TEXT,
    script             TEXT,
    release_group_mbid TEXT REFERENCES release_groups(mbid)
);`,
	`
CREATE TABLE IF NOT EXISTS release_artists (
    release_mbid TEXT NOT NULL REFERENCES releases(mbid),
    artist_mbid  TEXT NOT NULL,
    artist_name  TEXT NOT NULL,
    join_phrase  TEXT NOT NULL DEFAULT '',
    position     INTEGER NOT NULL,
    PRIMARY KEY (release_mbid, position)
);`,
	`
CREATE TABLE IF NOT EXISTS release_labels (
    release_mbid   TEXT NOT NULL REFERENCES releases(mbid),
    label_mbid     TEXT NOT NULL DEFAULT '',
    label_name     TEXT NOT NULL DEFAULT '',
    catalog_number TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (release_mbid, label_mbid, catalog_number)
);`,
	`
CREATE TABLE IF NOT EXISTS release_media (
    release_mbid TEXT NOT NULL REFERENCES releases(mbid),
    position     INTEGER NOT NULL,
    format       TEXT,
    track_count  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (release_mbid, position)
);`,
	`
CREATE TABLE IF NOT EXISTS recordings (
    mbid               TEXT PRIMARY KEY,
    title              TEXT NOT NULL,
    length             INTEGER,
    disambiguation     TEXT NOT NULL DEFAULT '',
    video              INTEGER NOT NULL DEFAULT 0,
    first_release_date TEXT
);`,
	`
CREATE TABLE IF NOT EXISTS recording_artists (
    recording_mbid TEXT NOT NULL REFERENCES recordings(mbid),
    artist_mbid    TEXT NOT NULL,
    artist_name    TEXT NOT NULL,
    join_phrase    TEXT NOT NULL DEFAULT '',
    position       INTEGER NOT NULL,
    PRIMARY KEY (recording_mbid, position)
);`,
	`
CREATE TABLE IF NOT EXISTS recording_isrcs (
    recording_mbid TEXT NOT NULL REFERENCES recordings(mbid),
    isrc           TEXT NOT NULL,
    PRIMARY KEY (recording_mbid, isrc)
);`,
	`
CREATE TABLE IF NOT EXISTS recording_tags (
    recording_mbid TEXT NOT NULL REFERENCES recordings(mbid),
    tag            TEXT NOT NULL,
    count          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (recording_mbid, tag)
);`,
	`
CREATE TABLE IF NOT EXISTS tracks (
    mbid           TEXT PRIMARY KEY,
    release_mbid   TEXT NOT NULL REFERENCES releases(mbid),
    recording_mbid TEXT NOT NULL REFERENCES recordings(mbid),
    media_position INTEGER NOT NULL,
    position       INTEGER NOT NULL,
    number         TEXT NOT NULL,
    title          TEXT NOT NULL,
    length         INTEGER
);`,
	`
CREATE TABLE IF NOT EXISTS external_links (
    entity_type TEXT NOT NULL,
    entity_mbid TEXT NOT NULL,
    rel_type    TEXT NOT NULL,
    url         TEXT NOT NULL,
    PRIMARY KEY (entity_mbid, rel_type, url)
);`,
}

var createIndexes = []string{
	`CREATE INDEX IF NOT EXISTS idx_artists_name ON artists(name);`,
	`CREATE INDEX IF NOT EXISTS idx_artists_sort_name ON artists(sort_name);`,
	`CREATE INDEX IF NOT EXISTS idx_artist_aliases_name ON artist_aliases(name);`,
	`CREATE INDEX IF NOT EXISTS idx_release_groups_title ON release_groups(title);`,
	`CREATE INDEX IF NOT EXISTS idx_release_group_artists_name ON release_group_artists(artist_name);`,
	`CREATE INDEX IF NOT EXISTS idx_releases_title ON releases(title);`,
	`CREATE INDEX IF NOT EXISTS idx_releases_release_group ON releases(release_group_mbid);`,
	`CREATE INDEX IF NOT EXISTS idx_releases_barcode ON releases(barcode) WHERE barcode IS NOT NULL AND barcode != '';`,
	`CREATE INDEX IF NOT EXISTS idx_release_artists_name ON release_artists(artist_name);`,
	`CREATE INDEX IF NOT EXISTS idx_release_labels_name ON release_labels(label_name);`,
	`CREATE INDEX IF NOT EXISTS idx_recordings_title ON recordings(title);`,
	`CREATE INDEX IF NOT EXISTS idx_recording_artists_name ON recording_artists(artist_name);`,
	`CREATE INDEX IF NOT EXISTS idx_recording_isrcs_isrc ON recording_isrcs(isrc);`,
	`CREATE INDEX IF NOT EXISTS idx_tracks_release ON tracks(release_mbid);`,
	`CREATE INDEX IF NOT EXISTS idx_tracks_recording ON tracks(recording_mbid);`,
	`CREATE INDEX IF NOT EXISTS idx_tracks_title ON tracks(title);`,
	`CREATE INDEX IF NOT EXISTS idx_external_links_entity ON external_links(entity_type, entity_mbid);`,
}

var buildPragmas = []string{
	`PRAGMA foreign_keys = OFF;`,
	`PRAGMA journal_mode = DELETE;`,
	`PRAGMA synchronous = OFF;`,
	`PRAGMA temp_store = MEMORY;`,
	`PRAGMA locking_mode = EXCLUSIVE;`,
}

func CreateSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, createMetaTable); err != nil {
		return err
	}
	for _, stmt := range createTables {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func CreateIndexes(ctx context.Context, db *sql.DB) error {
	for i, stmt := range createIndexes {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create index %d: %w", i, err)
		}
	}
	return nil
}

func ApplyBuildPragmas(ctx context.Context, db *sql.DB) error {
	for _, stmt := range buildPragmas {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func Finalize(ctx context.Context, db *sql.DB) error {
	for _, stmt := range []string{
		`PRAGMA optimize;`,
		`VACUUM;`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
