package parser

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func writeChunk(t *testing.T, dir, name string, lines []string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := zstd.NewWriter(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if _, err := encoder.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestScanChunkFilesReadsAllLinesAcrossWorkers(t *testing.T) {
	dir := t.TempDir()
	var paths []string
	var want []string
	for chunk := 0; chunk < 5; chunk++ {
		var lines []string
		for i := 0; i < 100; i++ {
			line := fmt.Sprintf(`{"chunk":%d,"row":%d}`, chunk, i)
			lines = append(lines, line)
			want = append(want, line)
		}
		paths = append(paths, writeChunk(t, dir, fmt.Sprintf("%06d.jsonl.zst", chunk), lines))
	}

	var mu sync.Mutex
	var got []string
	err := ScanChunkFiles(context.Background(), paths, 4, func(line []byte) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, string(line))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("got %d lines, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("line %d: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestScanChunkFilesPropagatesLineError(t *testing.T) {
	dir := t.TempDir()
	paths := []string{writeChunk(t, dir, "000000.jsonl.zst", []string{"a", "b"})}

	wantErr := fmt.Errorf("boom")
	err := ScanChunkFiles(context.Background(), paths, 2, func(line []byte) error {
		return wantErr
	})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestScanChunkFilesFailsOnCorruptChunk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000000.jsonl.zst")
	if err := os.WriteFile(path, []byte("not zstd data"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := ScanChunkFiles(context.Background(), []string{path}, 1, func(line []byte) error { return nil })
	if err == nil {
		t.Fatalf("expected error for corrupt chunk")
	}
}
