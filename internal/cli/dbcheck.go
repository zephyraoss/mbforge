package cli

import (
	"context"
	"database/sql"
	"fmt"
)

func ensureMBForgeSchema(ctx context.Context, db *sql.DB) error {
	requiredTables := []string{"artists", "release_groups", "releases", "recordings", "tracks"}
	for _, table := range requiredTables {
		var found string
		err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&found)
		if err == nil && found == table {
			continue
		}
		if err == sql.ErrNoRows {
			return fmt.Errorf("database does not look like an mbforge database: missing table %q", table)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
