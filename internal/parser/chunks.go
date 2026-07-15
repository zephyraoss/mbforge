package parser

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
)

func ScanChunkFiles(ctx context.Context, paths []string, concurrency int, lineFn func([]byte) error) error {
	if len(paths) == 0 {
		return fmt.Errorf("no chunk files to scan")
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(paths) {
		concurrency = len(paths)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan string)
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

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				if err := scanChunkFile(ctx, path, lineFn); err != nil {
					setErr(fmt.Errorf("scan chunk %s: %w", path, err))
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, path := range paths {
			select {
			case jobs <- path:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func scanChunkFile(ctx context.Context, path string, lineFn func([]byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	defer decoder.Close()

	return scanTarLines(ctx, decoder, lineFn)
}
