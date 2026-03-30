package libsqlutil

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/tursodatabase/go-libsql"
)

const DriverName = "libsql"

func OpenLocal(path string) (*sql.DB, error) {
	return sql.Open(DriverName, LocalDSN(path))
}

func LocalDSN(path string) string {
	if path == ":memory:" {
		return path
	}
	if strings.HasPrefix(path, "file:") || strings.HasPrefix(path, "libsql://") || strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	return "file:" + path
}

func ApplySoftHeapLimit(ctx context.Context, db *sql.DB, limit int64) (int64, error) {
	if limit < 0 {
		return -1, nil
	}

	var applied int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("PRAGMA soft_heap_limit=%d", limit)).Scan(&applied); err != nil {
		return 0, err
	}
	return applied, nil
}
