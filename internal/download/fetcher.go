package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

const (
	maxDownloadAttempts = 5
	initialRetryBackoff = time.Second
	maxRetryBackoff     = 10 * time.Second
)

type progressSink interface {
	Add64(int64) error
}

func FetchAll(ctx context.Context, client *http.Client, dump ResolvedDump, dumpDir string, verbose bool) (map[string]string, error) {
	if len(dump.Files) == 0 {
		return nil, fmt.Errorf("no dump files requested")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	targetDir := filepath.Join(dumpDir, dump.Directory)
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return nil, err
	}

	totalSize, completedSize := int64(0), int64(0)
	jobs := make([]downloadJob, 0, len(dump.Files))
	for _, file := range dump.Files {
		localPath := filepath.Join(targetDir, file.Name)
		size, _ := probeRemoteSize(ctx, client, file.URL)
		existing := existingBytes(localPath, size)
		totalSize += size
		completedSize += existing
		jobs = append(jobs, downloadJob{
			file:      file,
			localPath: localPath,
			size:      size,
		})
	}

	var bar progressSink
	if verbose && totalSize > 0 {
		pb := progressbar.NewOptions64(
			totalSize,
			progressbar.OptionSetDescription("MusicBrainz dump"),
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionShowBytes(true),
			progressbar.OptionSetWidth(20),
			progressbar.OptionThrottle(100*time.Millisecond),
			progressbar.OptionClearOnFinish(),
		)
		if completedSize > 0 {
			_ = pb.Add64(completedSize)
		}
		bar = pb
	}

	results := make(map[string]string, len(jobs))
	var mu sync.Mutex
	jobCh := make(chan downloadJob)
	errCh := make(chan error, 1)

	workers := min(len(jobs), 4)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if err := DownloadFile(ctx, client, job.file.URL, job.localPath, job.size, bar); err != nil {
					cancel()
					select {
					case errCh <- err:
					default:
					}
					return
				}
				mu.Lock()
				results[job.file.Entity] = job.localPath
				mu.Unlock()
			}
		}()
	}

	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		wg.Wait()
		select {
		case err := <-errCh:
			return nil, err
		default:
			return nil, ctx.Err()
		}
	case err := <-errCh:
		cancel()
		wg.Wait()
		return nil, err
	case <-done:
		return results, nil
	}
}

type downloadJob struct {
	file      File
	localPath string
	size      int64
}

func DownloadFile(ctx context.Context, client *http.Client, sourceURL, dst string, wantSize int64, sink progressSink) error {
	if info, err := os.Stat(dst); err == nil && (wantSize == 0 || info.Size() == wantSize) {
		log.Printf("cache hit for %s (%d bytes)", filepath.Base(dst), info.Size())
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	tmp := dst + ".part"
	if info, err := os.Stat(tmp); err == nil && wantSize > 0 && info.Size() == wantSize {
		if err := os.Rename(tmp, dst); err == nil {
			log.Printf("cache hit from partial for %s (%d bytes)", filepath.Base(dst), info.Size())
			return nil
		}
	}

	backoff := initialRetryBackoff
	for attempt := 1; attempt <= maxDownloadAttempts; attempt++ {
		retryable, err := downloadAttempt(ctx, client, sourceURL, dst, tmp, wantSize, sink)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable || attempt == maxDownloadAttempts {
			return err
		}

		log.Printf("download retry attempt=%d/%d url=%s err=%v", attempt, maxDownloadAttempts, sourceURL, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxRetryBackoff {
			backoff *= 2
			if backoff > maxRetryBackoff {
				backoff = maxRetryBackoff
			}
		}
	}

	return fmt.Errorf("download %s: exhausted retries", sourceURL)
}

func downloadAttempt(ctx context.Context, client *http.Client, sourceURL, dst, tmp string, wantSize int64, sink progressSink) (bool, error) {
	resumeFrom, err := currentPartialSize(tmp, wantSize)
	if err != nil {
		return false, err
	}

	logStarted(sourceURL, resumeFrom)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", "mbforge/1")
	if resumeFrom > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", resumeFrom))
	}

	resp, err := client.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if resumeFrom > 0 {
			resumeFrom = 0
		}
	case http.StatusPartialContent:
	case http.StatusRequestedRangeNotSatisfiable:
		if wantSize > 0 {
			if info, err := os.Stat(tmp); err == nil && info.Size() == wantSize {
				if err := os.Rename(tmp, dst); err == nil {
					log.Printf("download completed: %s (%d bytes)", sourceURL, info.Size())
					return false, nil
				}
			}
		}
		_ = os.Remove(tmp)
		return true, fmt.Errorf("download %s: range rejected, restarting", sourceURL)
	default:
		if shouldRetryStatus(resp.StatusCode) {
			return true, fmt.Errorf("download %s: unexpected status %s", sourceURL, resp.Status)
		}
		return false, fmt.Errorf("download %s: unexpected status %s", sourceURL, resp.Status)
	}

	file, err := openPartialFile(tmp, resumeFrom, resp.StatusCode == http.StatusPartialContent)
	if err != nil {
		return false, err
	}

	reader := io.Reader(resp.Body)
	if sink != nil {
		reader = io.TeeReader(resp.Body, progressWriter{sink: sink})
	}

	n, copyErr := io.Copy(file, reader)
	closeErr := file.Close()
	if copyErr != nil {
		return isRetryableBodyError(copyErr), copyErr
	}
	if closeErr != nil {
		return true, closeErr
	}

	total := resumeFrom + n
	if wantSize > 0 && total != wantSize {
		return true, fmt.Errorf("download %s: got %d bytes, want %d", sourceURL, total, wantSize)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return false, err
	}

	log.Printf("download completed: %s (%d bytes)", sourceURL, total)
	return false, nil
}

func probeRemoteSize(ctx context.Context, client *http.Client, sourceURL string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, sourceURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "mbforge/1")

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("probe %s: unexpected status %s", sourceURL, resp.Status)
	}
	if resp.ContentLength <= 0 {
		return 0, fmt.Errorf("probe %s: unknown content length", sourceURL)
	}
	return resp.ContentLength, nil
}

func existingBytes(dst string, wantSize int64) int64 {
	if wantSize <= 0 {
		return 0
	}
	if info, err := os.Stat(dst); err == nil {
		if info.Size() <= wantSize {
			return info.Size()
		}
	}
	if info, err := os.Stat(dst + ".part"); err == nil {
		if info.Size() <= wantSize {
			return info.Size()
		}
	}
	return 0
}

type progressWriter struct {
	sink progressSink
}

func (w progressWriter) Write(p []byte) (int, error) {
	if err := w.sink.Add64(int64(len(p))); err != nil {
		return 0, err
	}
	return len(p), nil
}

func currentPartialSize(tmp string, wantSize int64) (int64, error) {
	info, err := os.Stat(tmp)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if wantSize > 0 && info.Size() > wantSize {
		if err := os.Remove(tmp); err != nil {
			return 0, err
		}
		return 0, nil
	}
	return info.Size(), nil
}

func openPartialFile(tmp string, resumeFrom int64, appendMode bool) (*os.File, error) {
	if appendMode && resumeFrom > 0 {
		return os.OpenFile(tmp, os.O_WRONLY|os.O_APPEND, 0o644)
	}
	return os.Create(tmp)
}

func logStarted(sourceURL string, resumeFrom int64) {
	if resumeFrom > 0 {
		log.Printf("download resumed: %s from byte %d", sourceURL, resumeFrom)
		return
	}
	log.Printf("download started: %s", sourceURL)
}

func shouldRetryStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooManyRequests ||
		status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func isRetryableBodyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if errors.Is(urlErr.Err, context.Canceled) || errors.Is(urlErr.Err, context.DeadlineExceeded) {
			return false
		}
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "stream error") ||
		strings.Contains(msg, "internal_error") ||
		strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "server closed idle connection")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
