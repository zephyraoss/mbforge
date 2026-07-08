package cli

import (
	"context"
	"database/sql"
	"fmt"
)

func ensureMBForgeSchema(ctx context.Context, db *sql.DB) error {
	requiredTables := []string{"artists", "release_groups", "releases", "recordings", "tracks"}
	for _, table := range requiredTables {
		found, err := tableExists(ctx, db, table)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("database does not look like an mbforge database: missing table %q", table)
		}
	}
	return nil
}

func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var found string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return found == table, nil
}
