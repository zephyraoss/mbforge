package replication

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeCopyField(t *testing.T) {
	cases := []struct {
		raw  string
		want string
		null bool
	}{
		{raw: `plain`, want: "plain"},
		{raw: `\N`, null: true},
		{raw: `a\tb`, want: "a\tb"},
		{raw: `a\nb`, want: "a\nb"},
		{raw: `a\\tb`, want: `a\tb`},
		{raw: `a\\\\b`, want: `a\\b`},
		{raw: `\x41\102c`, want: "ABc"},
		{raw: `{"name":"Merged\\tAway"}`, want: `{"name":"Merged\tAway"}`},
	}
	for _, tc := range cases {
		got, ok := decodeCopyField([]byte(tc.raw))
		if tc.null {
			if ok {
				t.Errorf("decodeCopyField(%q): expected null, got %q", tc.raw, got)
			}
			continue
		}
		if !ok {
			t.Errorf("decodeCopyField(%q): unexpected null", tc.raw)
			continue
		}
		if got != tc.want {
			t.Errorf("decodeCopyField(%q): got %q want %q", tc.raw, got, tc.want)
		}
	}
}

func writeTarPacket(t *testing.T, dir string, files map[string]string) string {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range []string{"TIMESTAMP", "SCHEMA_SEQUENCE", "REPLICATION_SEQUENCE", "DBMIRROR_VERSION", "mbdump/pending_data", "mbdump/pending_keys", "mbdump/pending_ts"} {
		content, ok := files[name]
		if !ok {
			continue
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatalf("write tar header %s: %v", name, err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("write tar entry %s: %v", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	path := filepath.Join(dir, "packet.tar")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	return path
}

func standardPacketFiles(pendingData string) map[string]string {
	return map[string]string{
		"TIMESTAMP":            "2026-07-15 02:00:05.303734+00\n",
		"SCHEMA_SEQUENCE":      "30\n",
		"REPLICATION_SEQUENCE": "170432\n",
		"DBMIRROR_VERSION":     "2\n",
		"mbdump/pending_data":  pendingData,
		"mbdump/pending_keys":  "musicbrainz.artist\t{id}\n",
		"mbdump/pending_ts":    "9001\t2026-07-15 02:00:03.112233+00\n",
	}
}

func TestReadPacketFileOrdersAndFilters(t *testing.T) {
	pendingData := strings.Join([]string{
		`101	musicbrainz.artist	u	9001	{"id":301,"gid":"target-gid","name":"Target","edits_pending":1}	{"id":301,"gid":"target-gid","name":"Target","edits_pending":0}	\N	\N`,
		`102	musicbrainz.artist	d	9001	{"id":300,"gid":"old-gid","name":"Old"}	\N	\N	\N`,
		`104	musicbrainz.edit	u	9001	{"id":1}	{"id":2}	\N	\N`,
		`96	musicbrainz.recording	d	9000	{"id":501,"gid":"rec-gid","name":"Song"}	\N	\N	\N`,
		`103	musicbrainz.artist_gid_redirect	i	9001	\N	{"gid":"old-gid","new_id":301,"created":"2026-07-15T02:00:03+00:00"}	\N	\N`,
	}, "\n") + "\n"

	path := writeTarPacket(t, t.TempDir(), standardPacketFiles(pendingData))
	packet, err := ReadPacketFile(path, KeepTable)
	if err != nil {
		t.Fatalf("ReadPacketFile: %v", err)
	}

	if packet.SchemaSequence != "30" {
		t.Errorf("SchemaSequence: got %q want %q", packet.SchemaSequence, "30")
	}
	if packet.ReplicationSequence != "170432" {
		t.Errorf("ReplicationSequence: got %q want %q", packet.ReplicationSequence, "170432")
	}
	if packet.Timestamp != "2026-07-15 02:00:05.303734+00" {
		t.Errorf("Timestamp: got %q", packet.Timestamp)
	}

	var got []string
	for _, change := range packet.Changes {
		got = append(got, change.Table+":"+change.Op)
	}
	want := []string{"recording:d", "artist:u", "artist:d", "artist_gid_redirect:i"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("changes: got %v want %v", got, want)
	}

	deleted := packet.Changes[2]
	if gid, _ := stringField(deleted.Old, "gid"); gid != "old-gid" {
		t.Errorf("deleted artist gid: got %q", gid)
	}
	if deleted.New != nil {
		t.Errorf("delete should have nil newdata")
	}
}

func TestReadPacketFileRejectsWrongVersion(t *testing.T) {
	files := standardPacketFiles("")
	files["DBMIRROR_VERSION"] = "1\n"
	path := writeTarPacket(t, t.TempDir(), files)
	if _, err := ReadPacketFile(path, KeepTable); err == nil || !strings.Contains(err.Error(), "DBMIRROR_VERSION") {
		t.Fatalf("expected DBMIRROR_VERSION error, got %v", err)
	}

	files = standardPacketFiles("")
	delete(files, "mbdump/pending_data")
	path = writeTarPacket(t, t.TempDir(), files)
	if _, err := ReadPacketFile(path, KeepTable); err == nil || !strings.Contains(err.Error(), "pending_data") {
		t.Fatalf("expected missing pending_data error, got %v", err)
	}
}

func TestReadPacketFileBzip2Fixture(t *testing.T) {
	packet, err := ReadPacketFile(filepath.Join("testdata", "replication-170432-v2.tar.bz2"), KeepTable)
	if err != nil {
		t.Fatalf("ReadPacketFile: %v", err)
	}
	if packet.ReplicationSequence != "170432" {
		t.Errorf("ReplicationSequence: got %q", packet.ReplicationSequence)
	}
	if len(packet.Changes) != 5 {
		t.Fatalf("changes: got %d want 5", len(packet.Changes))
	}

	events, err := ExtractEvents(packet.Changes)
	if err != nil {
		t.Fatalf("ExtractEvents: %v", err)
	}

	wantDeletions := []Deletion{
		{EntityType: "recording", MBID: "0a8e8d55-4b83-4d93-b31c-b7c9f8f5aeae"},
		{EntityType: "artist", MBID: "8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1"},
	}
	if len(events.Deletions) != len(wantDeletions) {
		t.Fatalf("deletions: got %v want %v", events.Deletions, wantDeletions)
	}
	for i, want := range wantDeletions {
		if events.Deletions[i] != want {
			t.Errorf("deletion %d: got %v want %v", i, events.Deletions[i], want)
		}
	}

	wantRedirects := []Redirect{
		{EntityType: "artist", OldMBID: "8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1", NewRowID: 301},
		{EntityType: "release_group", OldMBID: "3d2f52a4-4a3e-4cd9-8fd4-6cea4b21bd07", NewRowID: 88001},
	}
	if len(events.Redirects) != len(wantRedirects) {
		t.Fatalf("redirects: got %v want %v", events.Redirects, wantRedirects)
	}
	for i, want := range wantRedirects {
		if events.Redirects[i] != want {
			t.Errorf("redirect %d: got %v want %v", i, events.Redirects[i], want)
		}
	}

	if mbid := events.RowMBIDs["artist"][301]; mbid != "6f0c2c16-dd7e-4268-a484-bfbcb2d4c48b" {
		t.Errorf("harvested artist 301: got %q", mbid)
	}
	if mbid := events.RowMBIDs["artist"][300]; mbid != "8dc08d1f-e393-4f85-a5dd-fab2d72dc2a1" {
		t.Errorf("harvested artist 300: got %q", mbid)
	}

	var deletedArtist *Change
	for i := range packet.Changes {
		if packet.Changes[i].Table == "artist" && packet.Changes[i].Op == "d" {
			deletedArtist = &packet.Changes[i]
		}
	}
	if deletedArtist == nil {
		t.Fatalf("no artist delete in fixture")
	}
	if name, _ := stringField(deletedArtist.Old, "name"); name != "Merged\tAway" {
		t.Errorf("copy-escaped name: got %q want %q", name, "Merged\tAway")
	}
	if comment, _ := stringField(deletedArtist.Old, "comment"); comment != `back\slash` {
		t.Errorf("copy-escaped comment: got %q want %q", comment, `back\slash`)
	}
}
