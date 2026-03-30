package parser

import (
	"testing"

	"github.com/zephyraoss/mbforge/internal/model"
)

func TestMergeDumpMetadataKeepsLatestTimestamp(t *testing.T) {
	meta := model.DumpMetadata{
		ReplicationSequence: "123",
		SchemaSequence:      "9",
		JSONSchemaNumber:    "1",
		DumpTimestamp:       "2026-03-28 01:07:23.905928+00",
	}

	err := MergeDumpMetadata(&meta, model.DumpMetadata{
		ReplicationSequence: "123",
		SchemaSequence:      "9",
		JSONSchemaNumber:    "1",
		DumpTimestamp:       "2026-03-28 13:22:52.710738+00",
	})
	if err != nil {
		t.Fatalf("MergeDumpMetadata returned error: %v", err)
	}
	if meta.DumpTimestamp != "2026-03-28 13:22:52.710738+00" {
		t.Fatalf("unexpected dump timestamp: got %q", meta.DumpTimestamp)
	}
}

func TestMergeDumpMetadataStillRejectsReplicationMismatch(t *testing.T) {
	meta := model.DumpMetadata{
		ReplicationSequence: "123",
	}

	err := MergeDumpMetadata(&meta, model.DumpMetadata{
		ReplicationSequence: "124",
	})
	if err == nil {
		t.Fatalf("expected replication mismatch error")
	}
}
