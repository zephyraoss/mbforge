package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/zephyraoss/mbforge/internal/model"
)

const (
	opReplace = "INSERT OR REPLACE"
	opIgnore  = "INSERT OR IGNORE"
)

type UpsertWriter struct {
	ctx     context.Context
	tx      *sql.Tx
	specs   map[string]tableSpec
	stmts   map[string]*sql.Stmt
	changed map[string]map[string]struct{}
}

func NewUpsertWriter(ctx context.Context, tx *sql.Tx) *UpsertWriter {
	return &UpsertWriter{
		ctx:     ctx,
		tx:      tx,
		specs:   writerSpecs(),
		stmts:   make(map[string]*sql.Stmt),
		changed: make(map[string]map[string]struct{}),
	}
}

func (u *UpsertWriter) Close() error {
	if u == nil || u.stmts == nil {
		return nil
	}
	var firstErr error
	for _, stmt := range u.stmts {
		if err := stmt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	u.stmts = nil
	return firstErr
}

func (u *UpsertWriter) Changed() map[string][]string {
	out := make(map[string][]string, len(u.changed))
	for entityType, set := range u.changed {
		mbids := make([]string, 0, len(set))
		for mbid := range set {
			mbids = append(mbids, mbid)
		}
		sort.Strings(mbids)
		out[entityType] = mbids
	}
	return out
}

func (u *UpsertWriter) ApplyMutation(m model.Mutation, entity string) error {
	switch entity {
	case "artist":
		return u.applyArtist(m)
	case "label":
		return u.applyLabel(m)
	case "work":
		return u.applyWork(m)
	case "release-group":
		return u.applyReleaseGroup(m)
	case "release":
		return u.applyRelease(m)
	case "recording":
		return u.applyRecording(m)
	default:
		return fmt.Errorf("unsupported entity %q", entity)
	}
}

func (u *UpsertWriter) applyArtist(m model.Mutation) error {
	for _, row := range m.Artists {
		if err := u.execEach(row.MBID,
			`DELETE FROM artist_aliases WHERE artist_mbid = ?`,
			`DELETE FROM artist_tags WHERE artist_mbid = ?`,
			`DELETE FROM artist_genres WHERE artist_mbid = ?`,
			`DELETE FROM artist_relationships WHERE artist_mbid = ?`,
			`DELETE FROM external_links WHERE entity_type = 'artist' AND entity_mbid = ?`,
		); err != nil {
			return err
		}
		if _, err := u.insertRow("artists", opReplace, artistRowValues(row)); err != nil {
			return err
		}
		u.markChanged("artist", row.MBID)
	}
	for _, row := range m.ArtistAliases {
		if _, err := u.insertRow("artist_aliases", opIgnore, artistAliasRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.ArtistTags {
		if _, err := u.insertRow("artist_tags", opIgnore, artistTagRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.ArtistGenres {
		if _, err := u.insertRow("artist_genres", opIgnore, artistGenreRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.ArtistRelationships {
		if _, err := u.insertRow("artist_relationships", opIgnore, artistRelationshipRowValues(row)); err != nil {
			return err
		}
	}
	return u.insertExternalLinks(m.ExternalLinks, nil)
}

func (u *UpsertWriter) applyLabel(m model.Mutation) error {
	for _, row := range m.Labels {
		if err := u.execEach(row.MBID,
			`DELETE FROM label_aliases WHERE label_mbid = ?`,
			`DELETE FROM label_tags WHERE label_mbid = ?`,
			`DELETE FROM label_genres WHERE label_mbid = ?`,
			`DELETE FROM external_links WHERE entity_type = 'label' AND entity_mbid = ?`,
		); err != nil {
			return err
		}
		if _, err := u.insertRow("labels", opReplace, labelRowValues(row)); err != nil {
			return err
		}
		u.markChanged("label", row.MBID)
	}
	for _, row := range m.LabelAliases {
		if _, err := u.insertRow("label_aliases", opIgnore, labelAliasRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.LabelTags {
		if _, err := u.insertRow("label_tags", opIgnore, labelTagRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.LabelGenres {
		if _, err := u.insertRow("label_genres", opIgnore, labelGenreRowValues(row)); err != nil {
			return err
		}
	}
	return u.insertExternalLinks(m.ExternalLinks, nil)
}

func (u *UpsertWriter) applyWork(m model.Mutation) error {
	for _, row := range m.Works {
		if err := u.execEach(row.MBID,
			`DELETE FROM work_aliases WHERE work_mbid = ?`,
			`DELETE FROM work_iswcs WHERE work_mbid = ?`,
			`DELETE FROM work_tags WHERE work_mbid = ?`,
			`DELETE FROM recording_works WHERE work_mbid = ?`,
			`DELETE FROM external_links WHERE entity_type = 'work' AND entity_mbid = ?`,
		); err != nil {
			return err
		}
		if _, err := u.insertRow("works", opReplace, workRowValues(row)); err != nil {
			return err
		}
		u.markChanged("work", row.MBID)
	}
	for _, row := range m.WorkAliases {
		if _, err := u.insertRow("work_aliases", opIgnore, workAliasRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.WorkISWCs {
		if _, err := u.insertRow("work_iswcs", opIgnore, workISWCRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.WorkTags {
		if _, err := u.insertRow("work_tags", opIgnore, workTagRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.RecordingWorks {
		if _, err := u.insertRow("recording_works", opIgnore, recordingWorkRowValues(row)); err != nil {
			return err
		}
	}
	return u.insertExternalLinks(m.ExternalLinks, nil)
}

func (u *UpsertWriter) applyReleaseGroup(m model.Mutation) error {
	for _, row := range m.ReleaseGroups {
		if err := u.execEach(row.MBID,
			`DELETE FROM release_group_secondary_types WHERE release_group_mbid = ?`,
			`DELETE FROM release_group_artists WHERE release_group_mbid = ?`,
			`DELETE FROM release_group_tags WHERE release_group_mbid = ?`,
			`DELETE FROM external_links WHERE entity_type = 'release_group' AND entity_mbid = ?`,
		); err != nil {
			return err
		}
		if _, err := u.insertRow("release_groups", opReplace, releaseGroupRowValues(row)); err != nil {
			return err
		}
		u.markChanged("release_group", row.MBID)
	}
	for _, row := range m.ReleaseGroupSecondaryTypes {
		if _, err := u.insertRow("release_group_secondary_types", opIgnore, releaseGroupSecondaryTypeRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.ReleaseGroupArtists {
		if _, err := u.insertRow("release_group_artists", opIgnore, releaseGroupArtistRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.ReleaseGroupTags {
		if _, err := u.insertRow("release_group_tags", opIgnore, releaseGroupTagRowValues(row)); err != nil {
			return err
		}
	}
	return u.insertExternalLinks(m.ExternalLinks, nil)
}

func (u *UpsertWriter) applyRelease(m model.Mutation) error {
	for _, row := range m.Releases {
		oldTracks, err := u.selectStrings(`SELECT mbid FROM tracks WHERE release_mbid = ?`, row.MBID)
		if err != nil {
			return err
		}
		for _, trackMBID := range oldTracks {
			u.markChanged("track", trackMBID)
		}
		if err := u.execEach(row.MBID,
			`DELETE FROM release_artists WHERE release_mbid = ?`,
			`DELETE FROM release_labels WHERE release_mbid = ?`,
			`DELETE FROM release_media WHERE release_mbid = ?`,
			`DELETE FROM tracks WHERE release_mbid = ?`,
			`DELETE FROM external_links WHERE entity_type = 'release' AND entity_mbid = ?`,
		); err != nil {
			return err
		}
		if _, err := u.insertRow("releases", opReplace, releaseRowValues(row)); err != nil {
			return err
		}
		u.markChanged("release", row.MBID)
	}
	for _, row := range m.ReleaseArtists {
		if _, err := u.insertRow("release_artists", opIgnore, releaseArtistRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.ReleaseLabels {
		if _, err := u.insertRow("release_labels", opIgnore, releaseLabelRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.ReleaseMedia {
		if _, err := u.insertRow("release_media", opIgnore, releaseMediaRowValues(row)); err != nil {
			return err
		}
	}
	newRecordings := make(map[string]struct{})
	for _, row := range m.Recordings {
		res, err := u.insertRow("recordings", opIgnore, recordingRowValues(row))
		if err != nil {
			return err
		}
		inserted, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if inserted > 0 {
			newRecordings[row.MBID] = struct{}{}
			u.markChanged("recording", row.MBID)
		}
	}
	for _, row := range m.RecordingArtists {
		if _, ok := newRecordings[row.RecordingMBID]; !ok {
			continue
		}
		if _, err := u.insertRow("recording_artists", opIgnore, recordingArtistRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.RecordingISRCs {
		if _, ok := newRecordings[row.RecordingMBID]; !ok {
			continue
		}
		if _, err := u.insertRow("recording_isrcs", opIgnore, recordingISRCRowValues(row)); err != nil {
			return err
		}
	}
	if _, err := u.insertEmbeddedWorks(m.Works, m.WorkISWCs); err != nil {
		return err
	}
	for _, row := range m.RecordingWorks {
		if _, ok := newRecordings[row.RecordingMBID]; !ok {
			continue
		}
		if _, err := u.insertRow("recording_works", opIgnore, recordingWorkRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.Tracks {
		if _, err := u.insertRow("tracks", opReplace, trackRowValues(row)); err != nil {
			return err
		}
		u.markChanged("track", row.MBID)
	}
	return u.insertExternalLinks(m.ExternalLinks, func(row model.ExternalLinkRow) bool {
		if row.EntityType == "release" {
			return true
		}
		_, ok := newRecordings[row.EntityMBID]
		return ok
	})
}

func (u *UpsertWriter) applyRecording(m model.Mutation) error {
	for _, row := range m.Recordings {
		if err := u.execEach(row.MBID,
			`DELETE FROM recording_artists WHERE recording_mbid = ?`,
			`DELETE FROM recording_isrcs WHERE recording_mbid = ?`,
			`DELETE FROM recording_tags WHERE recording_mbid = ?`,
			`DELETE FROM recording_works WHERE recording_mbid = ?`,
			`DELETE FROM external_links WHERE entity_type = 'recording' AND entity_mbid = ?`,
		); err != nil {
			return err
		}
		if _, err := u.insertRow("recordings", opReplace, recordingRowValues(row)); err != nil {
			return err
		}
		u.markChanged("recording", row.MBID)
	}
	for _, row := range m.RecordingArtists {
		if _, err := u.insertRow("recording_artists", opIgnore, recordingArtistRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.RecordingISRCs {
		if _, err := u.insertRow("recording_isrcs", opIgnore, recordingISRCRowValues(row)); err != nil {
			return err
		}
	}
	for _, row := range m.RecordingTags {
		if _, err := u.insertRow("recording_tags", opIgnore, recordingTagRowValues(row)); err != nil {
			return err
		}
	}
	if _, err := u.insertEmbeddedWorks(m.Works, m.WorkISWCs); err != nil {
		return err
	}
	for _, row := range m.RecordingWorks {
		if _, err := u.insertRow("recording_works", opIgnore, recordingWorkRowValues(row)); err != nil {
			return err
		}
	}
	return u.insertExternalLinks(m.ExternalLinks, nil)
}

func (u *UpsertWriter) insertEmbeddedWorks(works []model.WorkRow, iswcs []model.WorkISWCRow) (map[string]struct{}, error) {
	newWorks := make(map[string]struct{})
	for _, row := range works {
		res, err := u.insertRow("works", opIgnore, workRowValues(row))
		if err != nil {
			return nil, err
		}
		inserted, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if inserted > 0 {
			newWorks[row.MBID] = struct{}{}
			u.markChanged("work", row.MBID)
		}
	}
	for _, row := range iswcs {
		if _, ok := newWorks[row.WorkMBID]; !ok {
			continue
		}
		if _, err := u.insertRow("work_iswcs", opIgnore, workISWCRowValues(row)); err != nil {
			return nil, err
		}
	}
	return newWorks, nil
}

func (u *UpsertWriter) insertExternalLinks(rows []model.ExternalLinkRow, include func(model.ExternalLinkRow) bool) error {
	for _, row := range rows {
		if include != nil && !include(row) {
			continue
		}
		if _, err := u.insertRow("external_links", opIgnore, externalLinkRowValues(row)); err != nil {
			return err
		}
	}
	return nil
}

func (u *UpsertWriter) insertRow(specKey, op string, values []any) (sql.Result, error) {
	spec, ok := u.specs[specKey]
	if !ok {
		return nil, fmt.Errorf("unknown table spec %q", specKey)
	}
	res, err := u.exec(singleRowInsertSQL(spec, op), values...)
	if err != nil {
		return nil, fmt.Errorf("upsert into %s: %w", spec.table, err)
	}
	return res, nil
}

func (u *UpsertWriter) execEach(key string, stmts ...string) error {
	for _, sqlText := range stmts {
		if _, err := u.exec(sqlText, key); err != nil {
			return err
		}
	}
	return nil
}

func (u *UpsertWriter) exec(sqlText string, args ...any) (sql.Result, error) {
	stmt, err := u.stmt(sqlText)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(u.ctx, args...)
}

func (u *UpsertWriter) selectStrings(sqlText string, args ...any) ([]string, error) {
	stmt, err := u.stmt(sqlText)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(u.ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
}

func (u *UpsertWriter) stmt(sqlText string) (*sql.Stmt, error) {
	if u.stmts == nil {
		return nil, fmt.Errorf("upsert writer already closed")
	}
	if stmt, ok := u.stmts[sqlText]; ok {
		return stmt, nil
	}
	stmt, err := u.tx.PrepareContext(u.ctx, sqlText)
	if err != nil {
		return nil, err
	}
	u.stmts[sqlText] = stmt
	return stmt, nil
}

func (u *UpsertWriter) markChanged(entityType, mbid string) {
	set, ok := u.changed[entityType]
	if !ok {
		set = make(map[string]struct{})
		u.changed[entityType] = set
	}
	set[mbid] = struct{}{}
}

func singleRowInsertSQL(spec tableSpec, op string) string {
	var b strings.Builder
	b.WriteString(op)
	b.WriteString(" INTO ")
	b.WriteString(spec.table)
	b.WriteString(" (")
	b.WriteString(strings.Join(spec.columns, ", "))
	b.WriteString(") VALUES (")
	for i := range spec.columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('?')
	}
	b.WriteByte(')')
	return b.String()
}
