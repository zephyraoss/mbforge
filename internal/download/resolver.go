package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"
)

var SupportedEntities = []string{"artist", "release-group", "release", "recording"}

type File struct {
	Entity string
	Name   string
	URL    string
	Size   int64
}

type ResolvedDump struct {
	Mirror    string
	Directory string
	Files     []File
}

func ResolveLatest(ctx context.Context, client *http.Client, mirror string, entities []string) (ResolvedDump, error) {
	mirror = strings.TrimRight(mirror, "/")
	if mirror == "" {
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mirror+"/LATEST", nil)
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
		name := entity + ".tar.xz"
		files = append(files, File{
			Entity: entity,
			Name:   name,
			URL:    mirror + "/" + path.Join(dir, name),
		})
	}

	return ResolvedDump{
		Mirror:    mirror,
		Directory: dir,
		Files:     files,
	}, nil
}
