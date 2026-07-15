package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/zephyraoss/mbforge/internal/replication"
)

const MetaKeyPruneReplicationSequence = "prune_replication_sequence"

var createPruneTables = []string{
	`
CREATE TABLE IF NOT EXISTS gid_redirects (
    entity_type TEXT NOT NULL,
    old_mbid    TEXT NOT NULL,
    new_mbid    TEXT NOT NULL,
    PRIMARY KEY (entity_type, old_mbid)
);`,
	`
CREATE TABLE IF NOT EXISTS gid_redirects_pending (
    entity_type TEXT NOT NULL,
    old_mbid    TEXT NOT NULL,
    new_row_id  INTEGER NOT NULL,
    PRIMARY KEY (entity_type, old_mbid)
);`,
	`
CREATE TABLE IF NOT EXISTS mb_row_ids (
    entity_type TEXT NOT NULL,
    row_id      INTEGER NOT NULL,
    mbid        TEXT NOT NULL,
    PRIMARY KEY (entity_type, row_id)
);`,
}

func EnsurePruneSchema(ctx context.Context, db *sql.DB) error {
	for _, stmt := range createPruneTables {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

var pruneCascadeDeletes = map[string][]string{
	"artist": {
		`DELETE FROM artist_aliases WHERE artist_mbid = ?`,
		`DELETE FROM artist_tags WHERE artist_mbid = ?`,
		`DELETE FROM artist_genres WHERE artist_mbid = ?`,
		`DELETE FROM artist_relationships WHERE ? IN (artist_mbid, related_artist_mbid)`,
		`DELETE FROM external_links WHERE entity_type = 'artist' AND entity_mbid = ?`,
	},
	"label": {
		`DELETE FROM label_aliases WHERE label_mbid = ?`,
		`DELETE FROM label_tags WHERE label_mbid = ?`,
		`DELETE FROM label_genres WHERE label_mbid = ?`,
		`DELETE FROM external_links WHERE entity_type = 'label' AND entity_mbid = ?`,
	},
	"work": {
		`DELETE FROM work_aliases WHERE work_mbid = ?`,
		`DELETE FROM work_iswcs WHERE work_mbid = ?`,
		`DELETE FROM work_tags WHERE work_mbid = ?`,
		`DELETE FROM recording_works WHERE work_mbid = ?`,
		`DELETE FROM external_links WHERE entity_type = 'work' AND entity_mbid = ?`,
	},
	"release_group": {
		`DELETE FROM release_group_secondary_types WHERE release_group_mbid = ?`,
		`DELETE FROM release_group_artists WHERE release_group_mbid = ?`,
		`DELETE FROM release_group_tags WHERE release_group_mbid = ?`,
		`DELETE FROM external_links WHERE entity_type = 'release_group' AND entity_mbid = ?`,
	},
	"release": {
		`DELETE FROM release_artists WHERE release_mbid = ?`,
		`DELETE FROM release_labels WHERE release_mbid = ?`,
		`DELETE FROM release_media WHERE release_mbid = ?`,
		`DELETE FROM tracks WHERE release_mbid = ?`,
		`DELETE FROM external_links WHERE entity_type = 'release' AND entity_mbid = ?`,
	},
	"recording": {
		`DELETE FROM recording_artists WHERE recording_mbid = ?`,
		`DELETE FROM recording_isrcs WHERE recording_mbid = ?`,
		`DELETE FROM recording_tags WHERE recording_mbid = ?`,
		`DELETE FROM recording_works WHERE recording_mbid = ?`,
		`DELETE FROM external_links WHERE entity_type = 'recording' AND entity_mbid = ?`,
	},
	"track": {},
}

var pruneEntityDeletes = map[string]string{
	"artist":        `DELETE FROM artists WHERE mbid = ?`,
	"label":         `DELETE FROM labels WHERE mbid = ?`,
	"work":          `DELETE FROM works WHERE mbid = ?`,
	"release_group": `DELETE FROM release_groups WHERE mbid = ?`,
	"release":       `DELETE FROM releases WHERE mbid = ?`,
	"recording":     `DELETE FROM recordings WHERE mbid = ?`,
	"track":         `DELETE FROM tracks WHERE mbid = ?`,
}

type PruneResult struct {
	DeletedEntities  map[string]int
	RedirectsApplied int
	RedirectsPending int
	PendingResolved  int
}

func ApplyPruneEvents(ctx context.Context, tx *sql.Tx, events *replication.Events, hasSearchIndex, includeTracks bool) (PruneResult, error) {
	result := PruneResult{DeletedEntities: make(map[string]int)}

	if err := storeRowMBIDs(ctx, tx, events.RowMBIDs); err != nil {
		return result, err
	}

	changed := make(map[string][]string)
	for _, deletion := range events.Deletions {
		removed, trackMBIDs, err := deleteEntity(ctx, tx, deletion)
		if err != nil {
			return result, fmt.Errorf("delete %s %s: %w", deletion.EntityType, deletion.MBID, err)
		}
		if removed {
			result.DeletedEntities[deletion.EntityType]++
		}
		changed[deletion.EntityType] = append(changed[deletion.EntityType], deletion.MBID)
		changed["track"] = append(changed["track"], trackMBIDs...)
	}

	if hasSearchIndex {
		if err := RefreshSearchIndexRows(ctx, tx, changed, includeTracks); err != nil {
			return result, err
		}
	}

	for _, redirect := range events.Redirects {
		applied, err := applyRedirect(ctx, tx, redirect)
		if err != nil {
			return result, fmt.Errorf("redirect %s %s: %w", redirect.EntityType, redirect.OldMBID, err)
		}
		if applied {
			result.RedirectsApplied++
		} else {
			result.RedirectsPending++
		}
	}

	resolved, err := resolvePendingRedirects(ctx, tx)
	if err != nil {
		return result, err
	}
	result.PendingResolved = resolved
	return result, nil
}

func storeRowMBIDs(ctx context.Context, tx *sql.Tx, rowMBIDs map[string]map[int64]string) error {
	stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO mb_row_ids(entity_type, row_id, mbid) VALUES(?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for entityType, byRowID := range rowMBIDs {
		for rowID, mbid := range byRowID {
			if _, err := stmt.ExecContext(ctx, entityType, rowID, mbid); err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteEntity(ctx context.Context, tx *sql.Tx, deletion replication.Deletion) (bool, []string, error) {
	entityDelete, ok := pruneEntityDeletes[deletion.EntityType]
	if !ok {
		return false, nil, fmt.Errorf("unsupported entity type %q", deletion.EntityType)
	}

	var trackMBIDs []string
	if deletion.EntityType == "release" {
		var err error
		trackMBIDs, err = selectTrackMBIDs(ctx, tx, deletion.MBID)
		if err != nil {
			return false, nil, err
		}
	}

	for _, stmt := range pruneCascadeDeletes[deletion.EntityType] {
		if _, err := tx.ExecContext(ctx, stmt, deletion.MBID); err != nil {
			return false, nil, err
		}
	}

	res, err := tx.ExecContext(ctx, entityDelete, deletion.MBID)
	if err != nil {
		return false, nil, err
	}
	removed, err := res.RowsAffected()
	if err != nil {
		return false, nil, err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM mb_row_ids WHERE entity_type = ? AND mbid = ?`, deletion.EntityType, deletion.MBID); err != nil {
		return false, nil, err
	}
	return removed > 0, trackMBIDs, nil
}

func selectTrackMBIDs(ctx context.Context, tx *sql.Tx, releaseMBID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT mbid FROM tracks WHERE release_mbid = ?`, releaseMBID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mbids []string
	for rows.Next() {
		var mbid string
		if err := rows.Scan(&mbid); err != nil {
			return nil, err
		}
		mbids = append(mbids, mbid)
	}
	return mbids, rows.Err()
}

func applyRedirect(ctx context.Context, tx *sql.Tx, redirect replication.Redirect) (bool, error) {
	var newMBID string
	err := tx.QueryRowContext(ctx, `SELECT mbid FROM mb_row_ids WHERE entity_type = ? AND row_id = ?`,
		redirect.EntityType, redirect.NewRowID).Scan(&newMBID)
	if err == sql.ErrNoRows {
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO gid_redirects_pending(entity_type, old_mbid, new_row_id) VALUES(?, ?, ?)`,
			redirect.EntityType, redirect.OldMBID, redirect.NewRowID)
		return false, err
	}
	if err != nil {
		return false, err
	}
	return true, storeResolvedRedirect(ctx, tx, redirect.EntityType, redirect.OldMBID, newMBID)
}

func storeResolvedRedirect(ctx context.Context, tx *sql.Tx, entityType, oldMBID, newMBID string) error {
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO gid_redirects(entity_type, old_mbid, new_mbid) VALUES(?, ?, ?)`,
		entityType, oldMBID, newMBID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM gid_redirects_pending WHERE entity_type = ? AND old_mbid = ?`, entityType, oldMBID)
	return err
}

func resolvePendingRedirects(ctx context.Context, tx *sql.Tx) (int, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT p.entity_type, p.old_mbid, m.mbid
FROM gid_redirects_pending p
JOIN mb_row_ids m ON m.entity_type = p.entity_type AND m.row_id = p.new_row_id`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type resolved struct {
		entityType string
		oldMBID    string
		newMBID    string
	}
	var pending []resolved
	for rows.Next() {
		var r resolved
		if err := rows.Scan(&r.entityType, &r.oldMBID, &r.newMBID); err != nil {
			return 0, err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	rows.Close()

	for _, r := range pending {
		if err := storeResolvedRedirect(ctx, tx, r.entityType, r.oldMBID, r.newMBID); err != nil {
			return 0, err
		}
	}
	return len(pending), nil
}

type PendingRedirect struct {
	EntityType string
	OldMBID    string
	NewRowID   int64
}

func ListPendingRedirects(ctx context.Context, db *sql.DB, limit int) ([]PendingRedirect, error) {
	rows, err := db.QueryContext(ctx, `SELECT entity_type, old_mbid, new_row_id FROM gid_redirects_pending ORDER BY entity_type, old_mbid LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pending []PendingRedirect
	for rows.Next() {
		var p PendingRedirect
		if err := rows.Scan(&p.EntityType, &p.OldMBID, &p.NewRowID); err != nil {
			return nil, err
		}
		pending = append(pending, p)
	}
	return pending, rows.Err()
}

func CountPendingRedirects(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM gid_redirects_pending`).Scan(&count)
	return count, err
}

func ResolvePendingRedirect(ctx context.Context, db *sql.DB, pending PendingRedirect, newMBID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO mb_row_ids(entity_type, row_id, mbid) VALUES(?, ?, ?)`,
		pending.EntityType, pending.NewRowID, newMBID); err != nil {
		return err
	}
	if err := storeResolvedRedirect(ctx, tx, pending.EntityType, pending.OldMBID, newMBID); err != nil {
		return err
	}
	return tx.Commit()
}

func DeletePendingRedirect(ctx context.Context, db *sql.DB, pending PendingRedirect) error {
	_, err := db.ExecContext(ctx, `DELETE FROM gid_redirects_pending WHERE entity_type = ? AND old_mbid = ?`,
		pending.EntityType, pending.OldMBID)
	return err
}
