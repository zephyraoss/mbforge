package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/zephyraoss/mbforge/internal/model"
)

func WriteMeta(ctx context.Context, db *sql.DB, meta model.DumpMetadata) error {
	values := map[string]string{
		"replication_sequence": meta.ReplicationSequence,
		"schema_sequence":      meta.SchemaSequence,
		"json_schema_number":   meta.JSONSchemaNumber,
		"dump_timestamp":       meta.DumpTimestamp,
		"dump_dir":             meta.DumpDir,
		"imported_at":          time.Now().UTC().Format(time.RFC3339),
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM _meta`); err != nil {
		return err
	}
	for key, value := range values {
		if value == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO _meta(key, value) VALUES(?, ?)`, key, value); err != nil {
			return fmt.Errorf("insert meta %s: %w", key, err)
		}
	}
	return tx.Commit()
}

func ReadMeta(ctx context.Context, db *sql.DB) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT key, value FROM _meta ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		out[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
