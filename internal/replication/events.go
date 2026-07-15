package replication

import (
	"encoding/json"
	"fmt"
)

var entityTables = map[string]bool{
	"artist":        true,
	"label":         true,
	"work":          true,
	"release_group": true,
	"release":       true,
	"recording":     true,
	"track":         true,
}

var redirectTableEntity = map[string]string{
	"artist_gid_redirect":        "artist",
	"label_gid_redirect":         "label",
	"work_gid_redirect":          "work",
	"release_group_gid_redirect": "release_group",
	"release_gid_redirect":       "release",
	"recording_gid_redirect":     "recording",
}

func KeepTable(table string) bool {
	if entityTables[table] {
		return true
	}
	_, ok := redirectTableEntity[table]
	return ok
}

type Deletion struct {
	EntityType string
	MBID       string
}

type Redirect struct {
	EntityType string
	OldMBID    string
	NewRowID   int64
}

type Events struct {
	Deletions []Deletion
	Redirects []Redirect
	RowMBIDs  map[string]map[int64]string
}

func ExtractEvents(changes []Change) (*Events, error) {
	events := &Events{RowMBIDs: make(map[string]map[int64]string)}
	for _, change := range changes {
		if entityTables[change.Table] {
			if err := extractEntityChange(events, change); err != nil {
				return nil, err
			}
			continue
		}
		if entityType, ok := redirectTableEntity[change.Table]; ok {
			if err := extractRedirectChange(events, change, entityType); err != nil {
				return nil, err
			}
		}
	}
	return events, nil
}

func extractEntityChange(events *Events, change Change) error {
	if _, redirectable := redirectTableEntity[change.Table+"_gid_redirect"]; redirectable {
		harvestRowMBID(events, change.Table, change.Old)
		harvestRowMBID(events, change.Table, change.New)
	}
	if change.Op != "d" {
		return nil
	}
	mbid, ok := stringField(change.Old, "gid")
	if !ok {
		return fmt.Errorf("delete of %s (seqid %d) carries no gid in olddata", change.Table, change.SeqID)
	}
	events.Deletions = append(events.Deletions, Deletion{EntityType: change.Table, MBID: mbid})
	return nil
}

func extractRedirectChange(events *Events, change Change, entityType string) error {
	if change.Op != "i" && change.Op != "u" {
		return nil
	}
	oldMBID, ok := stringField(change.New, "gid")
	if !ok {
		return fmt.Errorf("%s %s (seqid %d) carries no gid in newdata", change.Table, change.Op, change.SeqID)
	}
	newRowID, ok := intField(change.New, "new_id")
	if !ok {
		return fmt.Errorf("%s %s (seqid %d) carries no new_id in newdata", change.Table, change.Op, change.SeqID)
	}
	events.Redirects = append(events.Redirects, Redirect{EntityType: entityType, OldMBID: oldMBID, NewRowID: newRowID})
	return nil
}

func harvestRowMBID(events *Events, entityType string, row map[string]any) {
	rowID, ok := intField(row, "id")
	if !ok {
		return
	}
	mbid, ok := stringField(row, "gid")
	if !ok {
		return
	}
	byRowID, ok := events.RowMBIDs[entityType]
	if !ok {
		byRowID = make(map[int64]string)
		events.RowMBIDs[entityType] = byRowID
	}
	byRowID[rowID] = mbid
}

func stringField(row map[string]any, key string) (string, bool) {
	value, ok := row[key].(string)
	return value, ok && value != ""
}

func intField(row map[string]any, key string) (int64, bool) {
	number, ok := row[key].(json.Number)
	if !ok {
		return 0, false
	}
	value, err := number.Int64()
	if err != nil {
		return 0, false
	}
	return value, true
}
