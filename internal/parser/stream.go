package parser

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/ulikunitz/xz"

	"github.com/zephyraoss/mbforge/internal/model"
)

const (
	metaJSONSchemaNumber       = "JSON_DUMPS_SCHEMA_NUMBER"
	metaReplicationSeq         = "REPLICATION_SEQUENCE"
	metaSchemaSeq              = "SCHEMA_SEQUENCE"
	metaTimestamp              = "TIMESTAMP"
	metaReadLimit        int64 = 1 << 20
)

func ScanEntityArchive(ctx context.Context, archivePath, entityName string, lineFn func([]byte) error) (model.DumpMetadata, error) {
	stream, err := openArchiveStream(ctx, archivePath)
	if err != nil {
		return model.DumpMetadata{}, err
	}
	defer stream.Close()

	tr := tar.NewReader(stream.Reader)

	target := path.Join("mbdump", entityName)
	var meta model.DumpMetadata
	var found bool

	for {
		select {
		case <-ctx.Done():
			return model.DumpMetadata{}, ctx.Err()
		default:
		}

		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return model.DumpMetadata{}, err
		}

		name := path.Clean(hdr.Name)
		switch name {
		case target:
			if err := scanTarLines(ctx, tr, lineFn); err != nil {
				return model.DumpMetadata{}, err
			}
			found = true
		case metaJSONSchemaNumber:
			meta.JSONSchemaNumber, err = readSmallTarFile(tr)
		case metaReplicationSeq:
			meta.ReplicationSequence, err = readSmallTarFile(tr)
		case metaSchemaSeq:
			meta.SchemaSequence, err = readSmallTarFile(tr)
		case metaTimestamp:
			meta.DumpTimestamp, err = readSmallTarFile(tr)
		default:
			continue
		}
		if err != nil {
			return model.DumpMetadata{}, err
		}
	}

	if !found {
		return model.DumpMetadata{}, fmt.Errorf("archive %s does not contain %s", archivePath, target)
	}
	return meta, nil
}

type archiveStream struct {
	Reader io.Reader
	Close  func() error
}

func openArchiveStream(ctx context.Context, archivePath string) (*archiveStream, error) {
	if xzPath, err := exec.LookPath("xz"); err == nil {
		cmd := exec.CommandContext(ctx, xzPath, "-dc", "--", archivePath)
		stdout, err := cmd.StdoutPipe()
		if err == nil {
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err == nil {
				return &archiveStream{
					Reader: stdout,
					Close: func() error {
						_ = stdout.Close()
						if err := cmd.Wait(); err != nil {
							msg := strings.TrimSpace(stderr.String())
							if msg != "" {
								return fmt.Errorf("xz failed for %s: %s: %w", archivePath, msg, err)
							}
							return fmt.Errorf("xz failed for %s: %w", archivePath, err)
						}
						return nil
					},
				}, nil
			}
		}
	}

	file, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	xzr, err := xz.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &archiveStream{
		Reader: xzr,
		Close:  file.Close,
	}, nil
}

func MergeDumpMetadata(dst *model.DumpMetadata, src model.DumpMetadata) error {
	if dst == nil {
		return nil
	}
	if err := mergeMetaField("replication_sequence", &dst.ReplicationSequence, src.ReplicationSequence); err != nil {
		return err
	}
	if err := mergeMetaField("schema_sequence", &dst.SchemaSequence, src.SchemaSequence); err != nil {
		return err
	}
	if err := mergeMetaField("json_schema_number", &dst.JSONSchemaNumber, src.JSONSchemaNumber); err != nil {
		return err
	}
	if err := mergeMetaField("dump_timestamp", &dst.DumpTimestamp, src.DumpTimestamp); err != nil {
		return err
	}
	return nil
}

func mergeMetaField(name string, dst *string, src string) error {
	if src == "" {
		return nil
	}
	if *dst == "" {
		*dst = src
		return nil
	}
	if *dst != src {
		return fmt.Errorf("dump metadata mismatch for %s: %q != %q", name, *dst, src)
	}
	return nil
}

func scanTarLines(ctx context.Context, r io.Reader, lineFn func([]byte) error) error {
	reader := bufio.NewReaderSize(r, 1<<20)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			line = bytesTrimSpace(line)
			if len(line) > 0 {
				if err := lineFn(line); err != nil {
					return err
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func readSmallTarFile(r io.Reader) (string, error) {
	data, err := io.ReadAll(io.LimitReader(r, metaReadLimit))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func bytesTrimSpace(in []byte) []byte {
	start, end := 0, len(in)
	for start < end && (in[start] == ' ' || in[start] == '\n' || in[start] == '\r' || in[start] == '\t') {
		start++
	}
	for end > start && (in[end-1] == ' ' || in[end-1] == '\n' || in[end-1] == '\r' || in[end-1] == '\t') {
		end--
	}
	return in[start:end]
}
