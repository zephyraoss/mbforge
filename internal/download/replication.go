package download

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const DefaultReplicationBaseURL = "https://metabrainz.org/api/musicbrainz"

var (
	ErrPacketEntityNotFound      = errors.New("packet has no dump for entity")
	ErrReplicationPacketNotFound = errors.New("replication packet not found")
)

var (
	packetNamePattern = regexp.MustCompile(`replication-(\d+)`)
	digitRunPattern   = regexp.MustCompile(`\d+`)
)

func FetchReplicationInfo(ctx context.Context, client *http.Client, baseURL, token string) (int, error) {
	endpoint := strings.TrimRight(baseURL, "/") + "/replication-info?token=" + url.QueryEscape(token)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "mbforge/1")

	resp, err := client.Do(req)
	if err != nil {
		return 0, redactToken(err, token)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return 0, fmt.Errorf("replication-info: %s: invalid or missing Live Data Feed token", resp.Status)
	default:
		return 0, fmt.Errorf("replication-info: unexpected status %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	var payload struct {
		LastPacket string `json:"last_packet"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("replication-info: decode response: %w", err)
	}
	sequence, err := parsePacketNumber(payload.LastPacket)
	if err != nil {
		return 0, fmt.Errorf("replication-info: %w", err)
	}
	return sequence, nil
}

func parsePacketNumber(lastPacket string) (int, error) {
	lastPacket = strings.TrimSpace(lastPacket)
	if lastPacket == "" {
		return 0, fmt.Errorf("response has no last_packet")
	}
	digits := ""
	if m := packetNamePattern.FindStringSubmatch(lastPacket); m != nil {
		digits = m[1]
	} else {
		digits = digitRunPattern.FindString(lastPacket)
	}
	if digits == "" {
		return 0, fmt.Errorf("cannot parse packet number from last_packet %q", lastPacket)
	}
	return strconv.Atoi(digits)
}

func PacketDirName(sequence int) string {
	return fmt.Sprintf("json-dump-%d", sequence)
}

func FetchPacketEntity(ctx context.Context, client *http.Client, baseURL, token string, sequence int, entity, dumpDir string) (string, error) {
	name := entity + ".tar.xz"
	targetDir := filepath.Join(dumpDir, PacketDirName(sequence))
	dst := filepath.Join(targetDir, name)
	if info, err := os.Stat(dst); err == nil && info.Size() > 0 {
		log.Printf("cache hit for %s (%d bytes)", filepath.Join(PacketDirName(sequence), name), info.Size())
		return dst, nil
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return "", err
	}

	displayURL := strings.TrimRight(baseURL, "/") + "/json-dumps/" + PacketDirName(sequence) + "/" + name
	sourceURL := displayURL + "?token=" + url.QueryEscape(token)
	return fetchWithRetries(ctx, client, sourceURL, displayURL, dst)
}

func ReplicationPacketName(sequence int) string {
	return fmt.Sprintf("replication-%d-v2.tar.bz2", sequence)
}

func FetchReplicationPacket(ctx context.Context, client *http.Client, baseURL, token string, sequence int, dumpDir string) (string, error) {
	name := ReplicationPacketName(sequence)
	dst := filepath.Join(dumpDir, name)
	if info, err := os.Stat(dst); err == nil && info.Size() > 0 {
		log.Printf("cache hit for %s (%d bytes)", name, info.Size())
		return dst, nil
	}
	if err := os.MkdirAll(dumpDir, 0o755); err != nil {
		return "", err
	}

	displayURL := strings.TrimRight(baseURL, "/") + "/" + name
	sourceURL := displayURL + "?token=" + url.QueryEscape(token)
	localPath, err := fetchWithRetries(ctx, client, sourceURL, displayURL, dst)
	if errors.Is(err, ErrPacketEntityNotFound) {
		return "", fmt.Errorf("%w: %s", ErrReplicationPacketNotFound, displayURL)
	}
	return localPath, err
}

func fetchWithRetries(ctx context.Context, client *http.Client, sourceURL, displayURL, dst string) (string, error) {
	backoff := initialRetryBackoff
	for attempt := 1; attempt <= maxDownloadAttempts; attempt++ {
		retryable, err := packetDownloadAttempt(ctx, client, sourceURL, displayURL, dst)
		if err == nil {
			return dst, nil
		}
		if errors.Is(err, ErrPacketEntityNotFound) {
			return "", err
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if !retryable || attempt == maxDownloadAttempts {
			return "", err
		}

		log.Printf("download retry attempt=%d/%d url=%s err=%v", attempt, maxDownloadAttempts, displayURL, err)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxRetryBackoff {
			backoff *= 2
			if backoff > maxRetryBackoff {
				backoff = maxRetryBackoff
			}
		}
	}
	return "", fmt.Errorf("download %s: exhausted retries", displayURL)
}

func packetDownloadAttempt(ctx context.Context, client *http.Client, sourceURL, displayURL, dst string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", "mbforge/1")

	resp, err := client.Do(req)
	if err != nil {
		return true, redactURL(err, sourceURL, displayURL)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return false, ErrPacketEntityNotFound
	case http.StatusUnauthorized, http.StatusForbidden:
		return false, fmt.Errorf("download %s: %s: invalid or missing Live Data Feed token", displayURL, resp.Status)
	default:
		return shouldRetryStatus(resp.StatusCode), fmt.Errorf("download %s: unexpected status %s", displayURL, resp.Status)
	}

	tmp := dst + ".part"
	file, err := os.Create(tmp)
	if err != nil {
		return false, err
	}
	n, copyErr := io.Copy(file, resp.Body)
	closeErr := file.Close()
	if copyErr != nil {
		return isRetryableBodyError(copyErr), redactURL(copyErr, sourceURL, displayURL)
	}
	if closeErr != nil {
		return true, closeErr
	}
	if resp.ContentLength > 0 && n != resp.ContentLength {
		return true, fmt.Errorf("download %s: got %d bytes, want %d", displayURL, n, resp.ContentLength)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return false, err
	}

	log.Printf("download completed: %s (%d bytes)", displayURL, n)
	return false, nil
}

func redactURL(err error, sourceURL, displayURL string) error {
	if err == nil {
		return nil
	}
	msg := strings.ReplaceAll(err.Error(), sourceURL, displayURL)
	if msg == err.Error() {
		return err
	}
	return errors.New(msg)
}

func redactToken(err error, token string) error {
	if err == nil || token == "" {
		return err
	}
	msg := err.Error()
	for _, needle := range []string{url.QueryEscape(token), token} {
		msg = strings.ReplaceAll(msg, needle, "REDACTED")
	}
	if msg == err.Error() {
		return err
	}
	return errors.New(msg)
}
