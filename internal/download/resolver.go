package download

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

var SupportedEntities = []string{"artist", "label", "work", "release-group", "release", "recording"}

const ChunkManifestFormat = "mbforge-chunks/1"

type File struct {
	Entity string
	Name   string
	URL    string
	Size   int64
	Chunks []ChunkRef
	Meta   ChunkMetadata
}

func (f File) Chunked() bool {
	return len(f.Chunks) > 0
}

type ChunkRef struct {
	Name string
	URL  string
	Size int64
}

type ChunkMetadata struct {
	JSONSchemaNumber    string `json:"json_dumps_schema_number"`
	ReplicationSequence string `json:"replication_sequence"`
	SchemaSequence      string `json:"schema_sequence"`
	DumpTimestamp       string `json:"timestamp"`
}

type ChunkManifest struct {
	Format   string        `json:"format"`
	Entity   string        `json:"entity"`
	Metadata ChunkMetadata `json:"metadata"`
	Chunks   []struct {
		Name string `json:"name"`
		Size int64  `json:"size"`
	} `json:"chunks"`
}

type ResolvedDump struct {
	Mirror    string
	Directory string
	Files     []File
}

func JoinURL(baseURL string, parts ...string) string {
	base, query, _ := strings.Cut(baseURL, "?")
	base = strings.TrimSuffix(base, "/")
	url := base + "/" + strings.Join(parts, "/")
	if query != "" {
		url += "?" + query
	}
	return url
}

func ResolveLatest(ctx context.Context, client *http.Client, mirror string, entities []string, probeChunks bool) (ResolvedDump, error) {
	if strings.TrimSuffix(strings.TrimSpace(mirror), "/") == "" {
		return ResolvedDump{}, fmt.Errorf("mirror is required")
	}

	if len(entities) == 0 {
		entities = slices.Clone(SupportedEntities)
	}
	for _, entity := range entities {
		if !slices.Contains(SupportedEntities, entity) {
			return ResolvedDump{}, fmt.Errorf("unsupported entity %q", entity)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, JoinURL(mirror, "LATEST"), nil)
	if err != nil {
		return ResolvedDump{}, err
	}
	req.Header.Set("User-Agent", "mbforge/1")

	resp, err := client.Do(req)
	if err != nil {
		return ResolvedDump{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ResolvedDump{}, fmt.Errorf("resolve latest dump: unexpected status %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return ResolvedDump{}, err
	}
	dir := strings.TrimSpace(string(body))
	if dir == "" {
		return ResolvedDump{}, fmt.Errorf("resolve latest dump: empty LATEST response")
	}

	files := make([]File, 0, len(entities))
	for _, entity := range entities {
		file := File{
			Entity: entity,
			Name:   entity + ".tar.xz",
			URL:    JoinURL(mirror, dir, entity+".tar.xz"),
		}
		if probeChunks {
			manifest, err := fetchChunkManifest(ctx, client, mirror, dir, entity)
			if err != nil {
				return ResolvedDump{}, err
			}
			if manifest != nil {
				file.Meta = manifest.Metadata
				file.Chunks = make([]ChunkRef, 0, len(manifest.Chunks))
				for _, chunk := range manifest.Chunks {
					file.Chunks = append(file.Chunks, ChunkRef{
						Name: chunk.Name,
						URL:  JoinURL(mirror, dir, entity+".chunks", chunk.Name),
						Size: chunk.Size,
					})
				}
			}
		}
		files = append(files, file)
	}

	return ResolvedDump{
		Mirror:    mirror,
		Directory: dir,
		Files:     files,
	}, nil
}

func fetchChunkManifest(ctx context.Context, client *http.Client, mirror, dir, entity string) (*ChunkManifest, error) {
	url := JoinURL(mirror, dir, entity+".chunks.json")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "mbforge/1")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("probe chunk manifest %s: unexpected status %s", url, resp.Status)
	}

	var manifest ChunkManifest
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode chunk manifest %s: %w", url, err)
	}
	if manifest.Format != ChunkManifestFormat {
		return nil, fmt.Errorf("chunk manifest %s: unsupported format %q", url, manifest.Format)
	}
	if len(manifest.Chunks) == 0 {
		return nil, fmt.Errorf("chunk manifest %s: no chunks listed", url)
	}
	for _, chunk := range manifest.Chunks {
		if chunk.Name == "" || strings.ContainsAny(chunk.Name, `/\`) || strings.Contains(chunk.Name, "..") {
			return nil, fmt.Errorf("chunk manifest %s: invalid chunk name %q", url, chunk.Name)
		}
	}
	return &manifest, nil
}
