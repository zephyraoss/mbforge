package cli

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	mbdb "github.com/zephyraoss/mbforge/internal/db"
	"github.com/zephyraoss/mbforge/internal/download"
	"github.com/zephyraoss/mbforge/internal/libsqlutil"
	"github.com/zephyraoss/mbforge/internal/model"
	"github.com/zephyraoss/mbforge/internal/parser"
)

var entityOrder = []string{"artist", "release-group", "release", "recording"}

type buildConfig struct {
	OutputPath string
	DumpDir    string
	KeepDumps  bool
	Workers    int
	BatchSize  int
	Entities   string
	Mirror     string
	Verbose    bool
}

func newBuildCmd() *cobra.Command {
	cfg := buildConfig{
		OutputPath: "./musicbrainz.db",
		DumpDir:    "./mbdump",
		Workers:    runtime.NumCPU(),
		BatchSize:  5000,
		Mirror:     "http://ftp.musicbrainz.org/pub/musicbrainz/data/json-dumps/",
	}

	cmd := &cobra.Command{
		Use:   "build",
		Short: "Download the latest MusicBrainz JSON dump and build a libSQL database",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signalContext(cmd.Context())
			defer cancel()
			return runBuild(ctx, cfg)
		},
	}

	cmd.Flags().StringVarP(&cfg.OutputPath, "output", "o", cfg.OutputPath, "Output database path")
	cmd.Flags().StringVar(&cfg.DumpDir, "dump-dir", cfg.DumpDir, "Directory to store downloaded dump files")
	cmd.Flags().BoolVar(&cfg.KeepDumps, "keep-dumps", cfg.KeepDumps, "Keep downloaded dump files after import")
	cmd.Flags().IntVarP(&cfg.Workers, "workers", "w", cfg.Workers, "Number of parallel JSON parse workers")
	cmd.Flags().IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Rows per INSERT batch")
	cmd.Flags().StringVarP(&cfg.Entities, "entities", "e", cfg.Entities, "Comma-separated entity types to import")
	cmd.Flags().StringVar(&cfg.Mirror, "mirror", cfg.Mirror, "Base URL for the MusicBrainz JSON dump mirror")
	cmd.Flags().BoolVarP(&cfg.Verbose, "verbose", "v", cfg.Verbose, "Verbose logging with progress bars")
	return cmd
}

func runBuild(ctx context.Context, cfg buildConfig) error {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}

	entities, err := parseEntityList(cfg.Entities)
	if err != nil {
		return err
	}
	log.Printf("build started output=%s dump_dir=%s workers=%d batch_size=%d entities=%s", cfg.OutputPath, cfg.DumpDir, cfg.Workers, cfg.BatchSize, strings.Join(entities, ","))

	if err := os.MkdirAll(filepath.Dir(cfg.OutputPath), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.DumpDir, 0o755); err != nil {
		return err
	}
	if err := removeExistingDB(cfg.OutputPath); err != nil {
		return err
	}

	client := defaultHTTPClient()
	resolved, err := download.ResolveLatest(ctx, client, cfg.Mirror, entities)
	if err != nil {
		return err
	}
	log.Printf("resolved latest dump=%s", resolved.Directory)

	localFiles, err := download.FetchAll(ctx, client, resolved, cfg.DumpDir, cfg.Verbose)
	if err != nil {
		return err
	}

	db, err := libsqlutil.OpenLocal(cfg.OutputPath)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := mbdb.ApplyBuildPragmas(ctx, db); err != nil {
		return err
	}
	if err := mbdb.CreateSchema(ctx, db); err != nil {
		return err
	}

	writer, err := mbdb.NewWriter(ctx, db, cfg.BatchSize)
	if err != nil {
		return err
	}
	defer writer.Rollback()

	meta := model.DumpMetadata{DumpDir: resolved.Directory}
	for _, entity := range entityOrder {
		localPath, ok := localFiles[entity]
		if !ok {
			continue
		}
		log.Printf("importing entity=%s archive=%s", entity, localPath)
		importMeta, err := importEntity(ctx, writer, entity, localPath, cfg.Workers)
		if err != nil {
			return err
		}
		if err := parser.MergeDumpMetadata(&meta, importMeta); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	if err := writer.Close(); err != nil {
		return err
	}
	if err := mbdb.CreateIndexes(ctx, db); err != nil {
		return err
	}
	if err := mbdb.WriteMeta(ctx, db, meta); err != nil {
		return err
	}
	if err := mbdb.Finalize(ctx, db); err != nil {
		return err
	}

	if !cfg.KeepDumps {
		if err := os.RemoveAll(filepath.Join(cfg.DumpDir, resolved.Directory)); err != nil {
			log.Printf("warning: cleanup dump cache: %v", err)
		}
	}

	log.Printf("build completed output=%s", cfg.OutputPath)
	return nil
}

type parseFunc func([]byte) (model.Mutation, error)

func importEntity(ctx context.Context, writer *mbdb.Writer, entity, archivePath string, workers int) (model.DumpMetadata, error) {
	parse, err := parserForEntity(entity)
	if err != nil {
		return model.DumpMetadata{}, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lines := make(chan []byte, workers*4)
	mutations := make(chan model.Mutation, workers*2)

	var meta model.DumpMetadata
	var firstErr error
	var errOnce sync.Once
	setErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	var scanWG sync.WaitGroup
	scanWG.Add(1)
	go func() {
		defer scanWG.Done()
		defer close(lines)
		scanMeta, err := parser.ScanEntityArchive(ctx, archivePath, entity, func(line []byte) error {
			payload := append([]byte(nil), line...)
			select {
			case lines <- payload:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil {
			setErr(err)
			return
		}
		meta = scanMeta
	}()

	var parseWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		parseWG.Add(1)
		go func() {
			defer parseWG.Done()
			for line := range lines {
				mutation, err := parse(line)
				if err != nil {
					setErr(err)
					return
				}
				if mutation.Empty() {
					continue
				}
				select {
				case mutations <- mutation:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		parseWG.Wait()
		close(mutations)
	}()

	for mutation := range mutations {
		if firstErr != nil {
			continue
		}
		if err := writer.WriteMutation(mutation); err != nil {
			setErr(err)
		}
	}

	scanWG.Wait()
	parseWG.Wait()
	if firstErr != nil {
		return model.DumpMetadata{}, firstErr
	}
	return meta, nil
}

func parserForEntity(entity string) (parseFunc, error) {
	switch entity {
	case "artist":
		return parser.ParseArtist, nil
	case "release-group":
		return parser.ParseReleaseGroup, nil
	case "release":
		return parser.ParseRelease, nil
	case "recording":
		return parser.ParseRecording, nil
	default:
		return nil, fmt.Errorf("unsupported entity %q", entity)
	}
}

func parseEntityList(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return slices.Clone(entityOrder), nil
	}

	seen := make(map[string]struct{})
	out := make([]string, 0, len(entityOrder))
	for _, part := range strings.Split(raw, ",") {
		entity := strings.TrimSpace(part)
		if entity == "" {
			continue
		}
		if !slices.Contains(download.SupportedEntities, entity) {
			return nil, fmt.Errorf("unsupported entity %q", entity)
		}
		if _, ok := seen[entity]; ok {
			continue
		}
		seen[entity] = struct{}{}
		out = append(out, entity)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no entities selected")
	}
	slices.SortFunc(out, func(a, b string) int {
		return slices.Index(entityOrder, a) - slices.Index(entityOrder, b)
	})
	return out, nil
}

func defaultHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{Transport: transport}
}

func removeExistingDB(path string) error {
	for _, target := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}
