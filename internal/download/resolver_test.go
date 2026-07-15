package download

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJoinURL(t *testing.T) {
	tests := []struct {
		base string
		want string
	}{
		{"http://ftp.musicbrainz.org/pub/musicbrainz/data/json-dumps/", "http://ftp.musicbrainz.org/pub/musicbrainz/data/json-dumps/20260711-001002/release.tar.xz"},
		{"http://ftp.musicbrainz.org/pub/musicbrainz/data/json-dumps", "http://ftp.musicbrainz.org/pub/musicbrainz/data/json-dumps/20260711-001002/release.tar.xz"},
		{"https://acct.blob.core.windows.net/mb-dumps?sv=2024&sig=abc", "https://acct.blob.core.windows.net/mb-dumps/20260711-001002/release.tar.xz?sv=2024&sig=abc"},
		{"https://acct.blob.core.windows.net/mb-dumps/?sv=2024&sig=abc", "https://acct.blob.core.windows.net/mb-dumps/20260711-001002/release.tar.xz?sv=2024&sig=abc"},
	}

	for _, tt := range tests {
		if got := JoinURL(tt.base, "20260711-001002", "release.tar.xz"); got != tt.want {
			t.Fatalf("JoinURL(%q) = %q, want %q", tt.base, got, tt.want)
		}
	}
}

func newMirrorServer(t *testing.T, requireSAS bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	guard := func(fn http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if requireSAS && r.URL.RawQuery != "sv=2024&sig=abc" {
				http.Error(w, "missing sas token", http.StatusForbidden)
				return
			}
			fn(w, r)
		}
	}

	mux.HandleFunc("/LATEST", guard(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("20260711-001002\n"))
	}))
	mux.HandleFunc("/20260711-001002/release.chunks.json", guard(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"format": ChunkManifestFormat,
			"entity": "release",
			"metadata": map[string]string{
				"json_dumps_schema_number": "1",
				"replication_sequence":     "999",
				"schema_sequence":          "30",
				"timestamp":                "2026-07-11 00:10:02+00",
			},
			"chunks": []map[string]any{
				{"name": "000000.jsonl.zst", "size": 11},
				{"name": "000001.jsonl.zst", "size": 22},
			},
		})
	}))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestResolveLatestProbesChunkManifests(t *testing.T) {
	srv := newMirrorServer(t, false)

	resolved, err := ResolveLatest(context.Background(), srv.Client(), srv.URL, []string{"release", "artist"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Directory != "20260711-001002" {
		t.Fatalf("directory = %q", resolved.Directory)
	}

	byEntity := map[string]File{}
	for _, file := range resolved.Files {
		byEntity[file.Entity] = file
	}

	release := byEntity["release"]
	if !release.Chunked() {
		t.Fatalf("release should be chunked")
	}
	if len(release.Chunks) != 2 {
		t.Fatalf("release chunks = %d, want 2", len(release.Chunks))
	}
	if release.Meta.ReplicationSequence != "999" {
		t.Fatalf("release replication sequence = %q", release.Meta.ReplicationSequence)
	}
	wantChunkURL := srv.URL + "/20260711-001002/release.chunks/000000.jsonl.zst"
	if release.Chunks[0].URL != wantChunkURL {
		t.Fatalf("chunk URL = %q, want %q", release.Chunks[0].URL, wantChunkURL)
	}

	artist := byEntity["artist"]
	if artist.Chunked() {
		t.Fatalf("artist should fall back to tar.xz")
	}
	if !strings.HasSuffix(artist.URL, "/20260711-001002/artist.tar.xz") {
		t.Fatalf("artist URL = %q", artist.URL)
	}
}

func TestResolveLatestKeepsSASQuery(t *testing.T) {
	srv := newMirrorServer(t, true)

	resolved, err := ResolveLatest(context.Background(), srv.Client(), srv.URL+"?sv=2024&sig=abc", []string{"release"}, true)
	if err != nil {
		t.Fatal(err)
	}

	release := resolved.Files[0]
	if !release.Chunked() {
		t.Fatalf("release should be chunked")
	}
	want := srv.URL + "/20260711-001002/release.chunks/000001.jsonl.zst?sv=2024&sig=abc"
	if release.Chunks[1].URL != want {
		t.Fatalf("chunk URL = %q, want %q", release.Chunks[1].URL, want)
	}
}

func TestResolveLatestSkipsProbeWhenDisabled(t *testing.T) {
	srv := newMirrorServer(t, false)

	resolved, err := ResolveLatest(context.Background(), srv.Client(), srv.URL, []string{"release"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Files[0].Chunked() {
		t.Fatalf("probe disabled but release resolved as chunked")
	}
}

func TestResolveLatestRejectsBadChunkNames(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/LATEST", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("20260711-001002\n"))
	})
	mux.HandleFunc("/20260711-001002/release.chunks.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"format": ChunkManifestFormat,
			"entity": "release",
			"chunks": []map[string]any{{"name": "../evil.zst", "size": 1}},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := ResolveLatest(context.Background(), srv.Client(), srv.URL, []string{"release"}, true)
	if err == nil || !strings.Contains(err.Error(), "invalid chunk name") {
		t.Fatalf("expected invalid chunk name error, got %v", err)
	}
}
