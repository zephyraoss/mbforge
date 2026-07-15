package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	mbdb "github.com/zephyraoss/mbforge/internal/db"
	"github.com/zephyraoss/mbforge/internal/download"
	"github.com/zephyraoss/mbforge/internal/libsqlutil"
	"github.com/zephyraoss/mbforge/internal/replication"
)

const (
	remoteResolveBaseURL   = "https://musicbrainz.org/ws/2"
	remoteResolveUserAgent = "mbforge/1 (+https://github.com/zephyraoss/oxygen)"
	remoteResolveMax       = 200
	remoteResolveDelay     = 1100 * time.Millisecond
)

type pruneConfig struct {
	DBPath        string
	Token         string
	BaseURL       string
	DumpDir       string
	KeepDumps     bool
	ResolveRemote bool
}

func newPruneCmd() *cobra.Command {
	cfg := pruneConfig{
		DBPath:        "./musicbrainz.db",
		BaseURL:       download.DefaultReplicationBaseURL,
		DumpDir:       "./mbdump",
		ResolveRemote: true,
	}

	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Apply entity deletions and merges from MusicBrainz RAW replication packets",
		Long: `Apply the two change types the incremental JSON dumps cannot convey: entity
deletions and merges (gid redirects). Reads the hourly RAW replication packets
(dbmirror2 v2 format) from the MusicBrainz Live Data Feed, deletes removed
entities (and their child and search-index rows), and records merges in a
gid_redirects table so old MBIDs can be forwarded to their merge target.

Resumes from _meta.prune_replication_sequence, seeded from
_meta.replication_sequence on the first run. Applies one packet per
transaction, so an interrupted prune can simply be rerun.

Requires a MetaBrainz Live Data Feed access token (--token or $` + ldfTokenEnv + `).

Must not run concurrently with "mbforge sync" against the same database: both
are single-writer jobs sharing the hourly Live Data Feed slot. Run them
sequentially, or use "mbforge sync --with-prune" to do both in one invocation.

Merge targets are referenced by MusicBrainz row id in the packets. Most
resolve from data in the same packet; the rest are looked up against the
MusicBrainz web service (disable with --resolve-remote=false) and retried on
later runs until resolved.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signalContext(cmd.Context())
			defer cancel()
			return runPrune(ctx, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Path to the database built by mbforge build")
	cmd.Flags().StringVar(&cfg.Token, "token", cfg.Token, "MetaBrainz Live Data Feed access token (defaults to $"+ldfTokenEnv+")")
	cmd.Flags().StringVar(&cfg.BaseURL, "base-url", cfg.BaseURL, "Base URL of the MetaBrainz API")
	cmd.Flags().StringVar(&cfg.DumpDir, "dump-dir", cfg.DumpDir, "Directory to store downloaded packet files")
	cmd.Flags().BoolVar(&cfg.KeepDumps, "keep-dumps", cfg.KeepDumps, "Keep downloaded packet files after import")
	cmd.Flags().BoolVar(&cfg.ResolveRemote, "resolve-remote", cfg.ResolveRemote, "Resolve merge targets missing from packet data via the MusicBrainz web service")
	return cmd
}

func runPrune(ctx context.Context, cfg pruneConfig) error {
	token, err := resolveLDFToken(cfg.Token)
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
	if err := mbdb.EnsurePruneSchema(ctx, db); err != nil {
		return err
	}
	if err := mbdb.ApplySyncPragmas(ctx, db); err != nil {
		return err
	}

	meta, err := mbdb.ReadMeta(ctx, db)
	if err != nil {
		return err
	}
	cursorSource := mbdb.MetaKeyPruneReplicationSequence
	cursorText := meta[mbdb.MetaKeyPruneReplicationSequence]
	if cursorText == "" {
		cursorSource = "replication_sequence"
		cursorText = meta["replication_sequence"]
	}
	cursor, err := strconv.Atoi(cursorText)
	if err != nil {
		return fmt.Errorf("database has no usable _meta.%s (%q): build it with `mbforge build` first", cursorSource, cursorText)
	}
	schemaSequence := meta["schema_sequence"]
	if schemaSequence == "" {
		return fmt.Errorf("database has no _meta.schema_sequence: build it with `mbforge build` first")
	}

	hasSearchIndex, err := mbdb.SearchIndexExists(ctx, db)
	if err != nil {
		return err
	}
	searchIndexTracks := mbdb.SearchIndexIncludesTracks(meta)

	client := defaultHTTPClient()
	current, err := download.FetchReplicationInfo(ctx, client, cfg.BaseURL, token)
	if err != nil {
		return err
	}
	log.Printf("prune started db=%s cursor=%d current=%d", cfg.DBPath, cursor, current)

	if current > cursor {
		if err := os.MkdirAll(cfg.DumpDir, 0o755); err != nil {
			return err
		}
		for seq := cursor + 1; seq <= current; seq++ {
			if err := applyPrunePacket(ctx, client, db, cfg, token, seq, schemaSequence, hasSearchIndex, searchIndexTracks); err != nil {
				return fmt.Errorf("apply replication packet %d: %w", seq, err)
			}
		}
	} else {
		log.Printf("database is up to date (%s=%d)", mbdb.MetaKeyPruneReplicationSequence, cursor)
	}

	if cfg.ResolveRemote {
		if err := resolvePendingRedirectsRemote(ctx, client, db); err != nil {
			return err
		}
	}
	if pending, err := mbdb.CountPendingRedirects(ctx, db); err != nil {
		return err
	} else if pending > 0 {
		log.Printf("%d gid redirects still pending resolution (will retry on the next run)", pending)
	}

	if err := mbdb.Optimize(ctx, db); err != nil {
		return err
	}
	log.Printf("prune completed db=%s %s=%d packets=%d", cfg.DBPath, mbdb.MetaKeyPruneReplicationSequence, max(cursor, current), max(0, current-cursor))
	return nil
}

func applyPrunePacket(ctx context.Context, client *http.Client, db *sql.DB, cfg pruneConfig, token string, seq int, schemaSequence string, hasSearchIndex, searchIndexTracks bool) error {
	packetStart := time.Now()

	localPath, err := download.FetchReplicationPacket(ctx, client, cfg.BaseURL, token, seq, cfg.DumpDir)
	if err != nil {
		return err
	}

	packet, err := replication.ReadPacketFile(localPath, replication.KeepTable)
	if err != nil {
		return err
	}
	if packet.SchemaSequence != "" && packet.SchemaSequence != schemaSequence {
		return fmt.Errorf("packet schema_sequence %s does not match database schema_sequence %s: the MusicBrainz schema changed, rebuild the database with `mbforge build`", packet.SchemaSequence, schemaSequence)
	}
	if packet.ReplicationSequence != "" && packet.ReplicationSequence != strconv.Itoa(seq) {
		return fmt.Errorf("packet reports replication_sequence %s, expected %d", packet.ReplicationSequence, seq)
	}

	events, err := replication.ExtractEvents(packet.Changes)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := mbdb.ApplyPruneEvents(ctx, tx, events, hasSearchIndex, searchIndexTracks)
	if err != nil {
		return err
	}
	if err := mbdb.UpdateMetaInTx(ctx, tx, map[string]string{
		mbdb.MetaKeyPruneReplicationSequence: strconv.Itoa(seq),
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("packet=%d pruned artists=%d labels=%d works=%d release_groups=%d releases=%d recordings=%d tracks=%d redirects=%d pending=%d resolved=%d elapsed=%s",
		seq, result.DeletedEntities["artist"], result.DeletedEntities["label"], result.DeletedEntities["work"],
		result.DeletedEntities["release_group"], result.DeletedEntities["release"],
		result.DeletedEntities["recording"], result.DeletedEntities["track"],
		result.RedirectsApplied, result.RedirectsPending, result.PendingResolved,
		time.Since(packetStart).Round(time.Millisecond))

	if !cfg.KeepDumps {
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			log.Printf("warning: cleanup packet cache: %v", err)
		}
	}
	return nil
}

func resolvePendingRedirectsRemote(ctx context.Context, client *http.Client, db *sql.DB) error {
	pending, err := mbdb.ListPendingRedirects(ctx, db, remoteResolveMax)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	log.Printf("resolving %d pending gid redirects via the MusicBrainz web service", len(pending))

	for i, p := range pending {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(remoteResolveDelay):
			}
		}

		newMBID, found, err := lookupCurrentMBID(ctx, client, p.EntityType, p.OldMBID)
		if err != nil {
			log.Printf("remote resolve stopped: %v", err)
			return nil
		}
		if !found {
			log.Printf("redirect %s %s: target no longer exists, dropping", p.EntityType, p.OldMBID)
			if err := mbdb.DeletePendingRedirect(ctx, db, p); err != nil {
				return err
			}
			continue
		}
		if newMBID == p.OldMBID {
			log.Printf("redirect %s %s: MBID resolves to itself, dropping", p.EntityType, p.OldMBID)
			if err := mbdb.DeletePendingRedirect(ctx, db, p); err != nil {
				return err
			}
			continue
		}
		if err := mbdb.ResolvePendingRedirect(ctx, db, p, newMBID); err != nil {
			return err
		}
	}
	return nil
}

func lookupCurrentMBID(ctx context.Context, client *http.Client, entityType, mbid string) (string, bool, error) {
	endpoint := remoteResolveBaseURL + "/" + strings.ReplaceAll(entityType, "_", "-") + "/" + mbid + "?fmt=json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("User-Agent", remoteResolveUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("lookup %s: unexpected status %s", endpoint, resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", false, err
	}
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", false, fmt.Errorf("lookup %s: decode response: %w", endpoint, err)
	}
	if payload.ID == "" {
		return "", false, fmt.Errorf("lookup %s: response has no id", endpoint)
	}
	return payload.ID, true, nil
}

func resolveLDFToken(flagValue string) (string, error) {
	token := flagValue
	if token == "" {
		token = os.Getenv(ldfTokenEnv)
	}
	if token == "" {
		return "", fmt.Errorf("a Live Data Feed token is required: pass --token or set %s", ldfTokenEnv)
	}
	return token, nil
}
