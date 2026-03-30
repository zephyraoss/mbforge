package cli

import (
	"context"
	"log"

	"github.com/spf13/cobra"

	mbdb "github.com/zephyraoss/mbforge/internal/db"
	"github.com/zephyraoss/mbforge/internal/libsqlutil"
)

type searchIndexConfig struct {
	DBPath string
}

func newSearchIndexCmd() *cobra.Command {
	cfg := searchIndexConfig{
		DBPath: "./musicbrainz.db",
	}

	cmd := &cobra.Command{
		Use:   "search-index",
		Short: "Build or rebuild the full-text search index on an existing database",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSearchIndex(cmd.Context(), cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Path to the database")
	return cmd
}

func runSearchIndex(ctx context.Context, cfg searchIndexConfig) error {
	db, err := libsqlutil.OpenLocal(cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := ensureMBForgeSchema(ctx, db); err != nil {
		return err
	}

	return mbdb.RebuildSearchIndex(ctx, db, log.Printf)
}
