package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	mbdb "github.com/zephyraoss/mbforge/internal/db"
	"github.com/zephyraoss/mbforge/internal/libsqlutil"
)

type infoConfig struct {
	DBPath string
}

func newInfoCmd() *cobra.Command {
	cfg := infoConfig{
		DBPath: "./musicbrainz.db",
	}

	cmd := &cobra.Command{
		Use:   "info",
		Short: "Show metadata about an existing mbforge database",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInfo(cmd.Context(), cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Path to the database")
	return cmd
}

func runInfo(ctx context.Context, cfg infoConfig) error {
	db, err := libsqlutil.OpenLocal(cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := ensureMBForgeSchema(ctx, db); err != nil {
		return err
	}

	meta, err := mbdb.ReadMeta(ctx, db)
	if err != nil {
		return err
	}
	hasSearchIndex, err := mbdb.SearchIndexExists(ctx, db)
	if err != nil {
		return err
	}

	info, err := os.Stat(cfg.DBPath)
	if err != nil {
		return err
	}

	counts := map[string]int64{}
	for _, table := range []string{"artists", "release_groups", "releases", "recordings", "tracks"} {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		var count int64
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return err
		}
		counts[table] = count
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "path\t%s\n", cfg.DBPath)
	fmt.Fprintf(w, "size_bytes\t%d\n", info.Size())
	fmt.Fprintf(w, "artists\t%d\n", counts["artists"])
	fmt.Fprintf(w, "release_groups\t%d\n", counts["release_groups"])
	fmt.Fprintf(w, "releases\t%d\n", counts["releases"])
	fmt.Fprintf(w, "recordings\t%d\n", counts["recordings"])
	fmt.Fprintf(w, "tracks\t%d\n", counts["tracks"])
	if hasSearchIndex {
		fmt.Fprintf(w, "search_index\tpresent\n")
	} else {
		fmt.Fprintf(w, "search_index\tabsent\n")
	}
	for _, key := range []string{"dump_dir", "replication_sequence", "schema_sequence", "json_schema_number", "dump_timestamp", "imported_at"} {
		if value := meta[key]; value != "" {
			fmt.Fprintf(w, "%s\t%s\n", key, value)
		}
	}
	return w.Flush()
}
