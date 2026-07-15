package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	mbdb "github.com/zephyraoss/mbforge/internal/db"
	"github.com/zephyraoss/mbforge/internal/download"
	"github.com/zephyraoss/mbforge/internal/libsqlutil"
	"github.com/zephyraoss/mbforge/internal/model"
	"github.com/zephyraoss/mbforge/internal/parser"
)

const ldfTokenEnv = "MBFORGE_LDF_TOKEN"

type syncConfig struct {
	DBPath    string
	Token     string
	BaseURL   string
	DumpDir   string
	KeepDumps bool
	Entities  string
	WithPrune bool
}

func newSyncCmd() *cobra.Command {
	cfg := syncConfig{
		DBPath:  "./musicbrainz.db",
		BaseURL: download.DefaultReplicationBaseURL,
		DumpDir: "./mbdump",
	}

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Apply MusicBrainz Live Data Feed incremental JSON dumps to an existing database",
		Long: `Apply MusicBrainz Live Data Feed incremental JSON dumps to a database built by
"mbforge build". Resumes from _meta.replication_sequence and applies one hourly
packet per transaction, so an interrupted sync can simply be rerun.

Requires a MetaBrainz Live Data Feed access token (--token or $` + ldfTokenEnv + `).

Incremental JSON dumps carry no deletions or merges; pass --with-prune to
apply those from the RAW replication packets in the same invocation (see
"mbforge prune"). Sync and prune must not run concurrently, so a single
invocation with --with-prune is the simplest way to run both hourly.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signalContext(cmd.Context())
			defer cancel()
			if err := runSync(ctx, cfg); err != nil {
				return err
			}
			if !cfg.WithPrune {
				return nil
			}
			return runPrune(ctx, pruneConfig{
				DBPath:        cfg.DBPath,
				Token:         cfg.Token,
				BaseURL:       cfg.BaseURL,
				DumpDir:       cfg.DumpDir,
				KeepDumps:     cfg.KeepDumps,
				ResolveRemote: true,
			})
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Path to the database built by mbforge build")
	cmd.Flags().StringVar(&cfg.Token, "token", cfg.Token, "MetaBrainz Live Data Feed access token (defaults to $"+ldfTokenEnv+")")
	cmd.Flags().StringVar(&cfg.BaseURL, "base-url", cfg.BaseURL, "Base URL of the MetaBrainz API")
	cmd.Flags().StringVar(&cfg.DumpDir, "dump-dir", cfg.DumpDir, "Directory to store downloaded packet files")
	cmd.Flags().BoolVar(&cfg.KeepDumps, "keep-dumps", cfg.KeepDumps, "Keep downloaded packet files after import")
	cmd.Flags().StringVarP(&cfg.Entities, "entities", "e", cfg.Entities, "Comma-separated entity types to sync")
	cmd.Flags().BoolVar(&cfg.WithPrune, "with-prune", cfg.WithPrune, "Run \"mbforge prune\" after applying the JSON dump packets")
	return cmd
}

func runSync(ctx context.Context, cfg syncConfig) error {
	token, err := resolveLDFToken(cfg.Token)
	if err != nil {
		return err
	}

	entities, err := parseEntityList(cfg.Entities)
	if err != nil {
		return err
	}

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
	if err := mbdb.CreateSchema(ctx, db); err != nil {
		return err
	}
	if err := mbdb.ApplySyncPragmas(ctx, db); err != nil {
		return err
	}

	meta, err := mbdb.ReadMeta(ctx, db)
	if err != nil {
		return err
	}
	cursor, err := strconv.Atoi(meta["replication_sequence"])
	if err != nil {
		return fmt.Errorf("database has no usable _meta.replication_sequence (%q): build it with `mbforge build` first", meta["replication_sequence"])
	}
	schemaSequence := meta["schema_sequence"]
	if schemaSequence == "" {
		return fmt.Errorf("database has no _meta.schema_sequence: build it with `mbforge build` first")
	}

	client := defaultHTTPClient()
	current, err := download.FetchReplicationInfo(ctx, client, cfg.BaseURL, token)
	if err != nil {
		return err
	}
	log.Printf("sync started db=%s cursor=%d current=%d entities=%s", cfg.DBPath, cursor, current, strings.Join(entities, ","))
	if current <= cursor {
		log.Printf("database is up to date (replication_sequence=%d)", cursor)
		return nil
	}

	hasSearchIndex, err := mbdb.SearchIndexExists(ctx, db)
	if err != nil {
		return err
	}
	searchIndexTracks := mbdb.SearchIndexIncludesTracks(meta)

	if err := os.MkdirAll(cfg.DumpDir, 0o755); err != nil {
		return err
	}

	for seq := cursor + 1; seq <= current; seq++ {
		if err := applySyncPacket(ctx, client, db, cfg, token, entities, seq, schemaSequence, hasSearchIndex, searchIndexTracks); err != nil {
			return fmt.Errorf("apply packet %d: %w", seq, err)
		}
	}

	if err := mbdb.Optimize(ctx, db); err != nil {
		return err
	}
	log.Printf("sync completed db=%s replication_sequence=%d packets=%d", cfg.DBPath, current, current-cursor)
	return nil
}

func applySyncPacket(ctx context.Context, client *http.Client, db *sql.DB, cfg syncConfig, token string, entities []string, seq int, schemaSequence string, hasSearchIndex, searchIndexTracks bool) error {
	packetStart := time.Now()

	localFiles := make(map[string]string, len(entities))
	for _, entity := range entities {
		localPath, err := download.FetchPacketEntity(ctx, client, cfg.BaseURL, token, seq, entity, cfg.DumpDir)
		if errors.Is(err, download.ErrPacketEntityNotFound) {
			log.Printf("packet=%d entity=%s no changes", seq, entity)
			continue
		}
		if err != nil {
			return err
		}
		localFiles[entity] = localPath
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	writer := mbdb.NewUpsertWriter(ctx, tx)
	defer writer.Close()

	var packetMeta model.DumpMetadata
	for _, entity := range entityOrder {
		localPath, ok := localFiles[entity]
		if !ok {
			continue
		}
		parse, err := parserForEntity(entity)
		if err != nil {
			return err
		}
		scanMeta, err := parser.ScanEntityArchive(ctx, localPath, entity, func(line []byte) error {
			mutation, err := parse(line)
			if err != nil {
				return err
			}
			return writer.ApplyMutation(mutation, entity)
		})
		if err != nil {
			return err
		}
		if err := parser.MergeDumpMetadata(&packetMeta, scanMeta); err != nil {
			return err
		}
	}

	if packetMeta.SchemaSequence != "" && packetMeta.SchemaSequence != schemaSequence {
		return fmt.Errorf("packet schema_sequence %s does not match database schema_sequence %s: the MusicBrainz schema changed, rebuild the database with `mbforge build`", packetMeta.SchemaSequence, schemaSequence)
	}
	if packetMeta.ReplicationSequence != "" && packetMeta.ReplicationSequence != strconv.Itoa(seq) {
		return fmt.Errorf("packet reports replication_sequence %s, expected %d", packetMeta.ReplicationSequence, seq)
	}

	changed := writer.Changed()
	if hasSearchIndex {
		if err := mbdb.RefreshSearchIndexRows(ctx, tx, changed, searchIndexTracks); err != nil {
			return err
		}
	}

	if err := mbdb.UpdateMetaInTx(ctx, tx, map[string]string{
		"replication_sequence": strconv.Itoa(seq),
		"dump_timestamp":       packetMeta.DumpTimestamp,
		"imported_at":          time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("packet=%d applied artists=%d labels=%d works=%d release_groups=%d releases=%d recordings=%d tracks=%d elapsed=%s",
		seq, len(changed["artist"]), len(changed["label"]), len(changed["work"]),
		len(changed["release_group"]), len(changed["release"]),
		len(changed["recording"]), len(changed["track"]), time.Since(packetStart).Round(time.Millisecond))

	if !cfg.KeepDumps {
		if err := os.RemoveAll(filepath.Join(cfg.DumpDir, download.PacketDirName(seq))); err != nil {
			log.Printf("warning: cleanup packet cache: %v", err)
		}
	}
	return nil
}
