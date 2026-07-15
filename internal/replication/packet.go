package replication

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

const musicbrainzSchemaPrefix = "musicbrainz."

type Change struct {
	SeqID int64
	XID   int64
	Op    string
	Table string
	Old   map[string]any
	New   map[string]any
}

type Packet struct {
	SchemaSequence      string
	ReplicationSequence string
	Timestamp           string
	Changes             []Change
}

func ReadPacketFile(path string, keep func(table string) bool) (*Packet, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buffered := bufio.NewReaderSize(file, 1<<20)
	magic, err := buffered.Peek(3)
	if err != nil {
		return nil, fmt.Errorf("read packet %s: %w", path, err)
	}
	var reader io.Reader = buffered
	if bytes.Equal(magic, []byte("BZh")) {
		reader = bzip2.NewReader(buffered)
	}

	packet, err := readPacket(reader, keep)
	if err != nil {
		return nil, fmt.Errorf("read packet %s: %w", path, err)
	}
	return packet, nil
}

func readPacket(r io.Reader, keep func(table string) bool) (*Packet, error) {
	archive := tar.NewReader(r)
	packet := &Packet{}
	dbmirrorVersion := ""
	sawPendingData := false

	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}

		switch strings.TrimPrefix(header.Name, "./") {
		case "SCHEMA_SEQUENCE":
			packet.SchemaSequence, err = readTrimmedFile(archive)
		case "REPLICATION_SEQUENCE":
			packet.ReplicationSequence, err = readTrimmedFile(archive)
		case "TIMESTAMP":
			packet.Timestamp, err = readTrimmedFile(archive)
		case "DBMIRROR_VERSION":
			dbmirrorVersion, err = readTrimmedFile(archive)
		case "mbdump/pending_data":
			sawPendingData = true
			packet.Changes, err = parsePendingData(archive, keep)
		}
		if err != nil {
			return nil, err
		}
	}

	if !sawPendingData {
		return nil, fmt.Errorf("no mbdump/pending_data in archive: not a dbmirror2 replication packet")
	}
	if dbmirrorVersion != "2" {
		return nil, fmt.Errorf("DBMIRROR_VERSION is %q, expected 2", dbmirrorVersion)
	}
	orderByTransaction(packet.Changes)
	return packet, nil
}

func readTrimmedFile(r io.Reader) (string, error) {
	data, err := io.ReadAll(io.LimitReader(r, 1<<16))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func parsePendingData(r io.Reader, keep func(table string) bool) ([]Change, error) {
	buffered := bufio.NewReaderSize(r, 1<<20)
	var changes []Change
	for lineNumber := 1; ; lineNumber++ {
		line, err := buffered.ReadBytes('\n')
		line = bytes.TrimSuffix(line, []byte{'\n'})
		if len(line) > 0 && !bytes.Equal(line, []byte(`\.`)) {
			change, kept, parseErr := parsePendingLine(line, keep)
			if parseErr != nil {
				return nil, fmt.Errorf("pending_data line %d: %w", lineNumber, parseErr)
			}
			if kept {
				changes = append(changes, change)
			}
		}
		if err == io.EOF {
			return changes, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func parsePendingLine(line []byte, keep func(table string) bool) (Change, bool, error) {
	fields := bytes.Split(line, []byte{'\t'})
	if len(fields) < 6 {
		return Change{}, false, fmt.Errorf("has %d fields, expected at least 6 (seqid, tablename, op, xid, olddata, newdata)", len(fields))
	}

	tableName, ok := decodeCopyField(fields[1])
	if !ok {
		return Change{}, false, fmt.Errorf("tablename is null")
	}
	if !strings.HasPrefix(tableName, musicbrainzSchemaPrefix) {
		return Change{}, false, nil
	}
	table := strings.TrimPrefix(tableName, musicbrainzSchemaPrefix)
	if !keep(table) {
		return Change{}, false, nil
	}

	seqidText, _ := decodeCopyField(fields[0])
	seqid, err := strconv.ParseInt(seqidText, 10, 64)
	if err != nil {
		return Change{}, false, fmt.Errorf("bad seqid %q", seqidText)
	}
	op, _ := decodeCopyField(fields[2])
	if op != "i" && op != "u" && op != "d" {
		return Change{}, false, fmt.Errorf("bad op %q", op)
	}
	xidText, _ := decodeCopyField(fields[3])
	xid, err := strconv.ParseInt(xidText, 10, 64)
	if err != nil {
		return Change{}, false, fmt.Errorf("bad xid %q", xidText)
	}

	oldData, err := decodeRowJSON(fields[4])
	if err != nil {
		return Change{}, false, fmt.Errorf("olddata: %w", err)
	}
	newData, err := decodeRowJSON(fields[5])
	if err != nil {
		return Change{}, false, fmt.Errorf("newdata: %w", err)
	}

	return Change{SeqID: seqid, XID: xid, Op: op, Table: table, Old: oldData, New: newData}, true, nil
}

func decodeRowJSON(field []byte) (map[string]any, error) {
	text, ok := decodeCopyField(field)
	if !ok {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var row map[string]any
	if err := decoder.Decode(&row); err != nil {
		return nil, err
	}
	return row, nil
}

func orderByTransaction(changes []Change) {
	commitOrder := make(map[int64]int64, len(changes))
	for _, change := range changes {
		if change.SeqID > commitOrder[change.XID] {
			commitOrder[change.XID] = change.SeqID
		}
	}
	sort.SliceStable(changes, func(i, j int) bool {
		if commitOrder[changes[i].XID] != commitOrder[changes[j].XID] {
			return commitOrder[changes[i].XID] < commitOrder[changes[j].XID]
		}
		return changes[i].SeqID < changes[j].SeqID
	})
}
