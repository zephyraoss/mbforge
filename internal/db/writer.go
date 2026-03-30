package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/zephyraoss/mbforge/internal/model"
)

type tableSpec struct {
	key      string
	table    string
	columns  []string
	insertOp string
}

type tableBatch struct {
	spec tableSpec
	rows [][]any
}

type Writer struct {
	ctx       context.Context
	tx        *sql.Tx
	batchSize int
	batches   map[string]*tableBatch
	specs     map[string]tableSpec
}

func NewWriter(ctx context.Context, db *sql.DB, batchSize int) (*Writer, error) {
	if batchSize <= 0 {
		batchSize = 5000
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	specs := writerSpecs()
	batches := make(map[string]*tableBatch, len(specs))
	for key, spec := range specs {
		batches[key] = &tableBatch{spec: spec}
	}

	return &Writer{
		ctx:       ctx,
		tx:        tx,
		batchSize: batchSize,
		batches:   batches,
		specs:     specs,
	}, nil
}

func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	if w.tx == nil {
		return nil
	}
	if err := w.Flush(); err != nil {
		_ = w.tx.Rollback()
		w.tx = nil
		return err
	}
	err := w.tx.Commit()
	w.tx = nil
	return err
}

func (w *Writer) Rollback() error {
	if w == nil || w.tx == nil {
		return nil
	}
	err := w.tx.Rollback()
	w.tx = nil
	return err
}

func (w *Writer) Flush() error {
	if w == nil || w.tx == nil {
		return nil
	}
	for _, batch := range w.batches {
		if err := w.flushBatch(batch); err != nil {
			_ = w.tx.Rollback()
			w.tx = nil
			return err
		}
	}
	return nil
}

func (w *Writer) WriteMutation(m model.Mutation) error {
	if err := w.addArtistRows(m.Artists); err != nil {
		return err
	}
	if err := w.addArtistAliasRows(m.ArtistAliases); err != nil {
		return err
	}
	if err := w.addArtistTagRows(m.ArtistTags); err != nil {
		return err
	}
	if err := w.addArtistGenreRows(m.ArtistGenres); err != nil {
		return err
	}
	if err := w.addReleaseGroupRows(m.ReleaseGroups); err != nil {
		return err
	}
	if err := w.addReleaseGroupSecondaryTypeRows(m.ReleaseGroupSecondaryTypes); err != nil {
		return err
	}
	if err := w.addReleaseGroupArtistRows(m.ReleaseGroupArtists); err != nil {
		return err
	}
	if err := w.addReleaseGroupTagRows(m.ReleaseGroupTags); err != nil {
		return err
	}
	if err := w.addReleaseRows(m.Releases); err != nil {
		return err
	}
	if err := w.addReleaseArtistRows(m.ReleaseArtists); err != nil {
		return err
	}
	if err := w.addReleaseLabelRows(m.ReleaseLabels); err != nil {
		return err
	}
	if err := w.addReleaseMediaRows(m.ReleaseMedia); err != nil {
		return err
	}
	if err := w.addRecordingRows(m.Recordings); err != nil {
		return err
	}
	if err := w.addRecordingArtistRows(m.RecordingArtists); err != nil {
		return err
	}
	if err := w.addRecordingISRCRows(m.RecordingISRCs); err != nil {
		return err
	}
	if err := w.addRecordingTagRows(m.RecordingTags); err != nil {
		return err
	}
	if err := w.addTrackRows(m.Tracks); err != nil {
		return err
	}
	if err := w.addExternalLinkRows(m.ExternalLinks); err != nil {
		return err
	}
	return nil
}

func writerSpecs() map[string]tableSpec {
	return map[string]tableSpec{
		"artists":                       {key: "artists", table: "artists", insertOp: "INSERT", columns: []string{"mbid", "name", "sort_name", "disambiguation", "type", "country", "gender", "begin_date", "end_date", "ended", "area_mbid", "area_name"}},
		"artist_aliases":                {key: "artist_aliases", table: "artist_aliases", insertOp: "INSERT OR IGNORE", columns: []string{"artist_mbid", "name", "sort_name", "type", "locale", "is_primary"}},
		"artist_tags":                   {key: "artist_tags", table: "artist_tags", insertOp: "INSERT OR IGNORE", columns: []string{"artist_mbid", "tag", "count"}},
		"artist_genres":                 {key: "artist_genres", table: "artist_genres", insertOp: "INSERT OR IGNORE", columns: []string{"artist_mbid", "genre_mbid", "genre_name", "count"}},
		"release_groups":                {key: "release_groups", table: "release_groups", insertOp: "INSERT", columns: []string{"mbid", "title", "primary_type", "disambiguation", "first_release_date"}},
		"release_group_secondary_types": {key: "release_group_secondary_types", table: "release_group_secondary_types", insertOp: "INSERT OR IGNORE", columns: []string{"release_group_mbid", "type"}},
		"release_group_artists":         {key: "release_group_artists", table: "release_group_artists", insertOp: "INSERT OR IGNORE", columns: []string{"release_group_mbid", "artist_mbid", "artist_name", "join_phrase", "position"}},
		"release_group_tags":            {key: "release_group_tags", table: "release_group_tags", insertOp: "INSERT OR IGNORE", columns: []string{"release_group_mbid", "tag", "count"}},
		"releases":                      {key: "releases", table: "releases", insertOp: "INSERT", columns: []string{"mbid", "title", "status", "date", "country", "barcode", "packaging", "language", "script", "release_group_mbid"}},
		"release_artists":               {key: "release_artists", table: "release_artists", insertOp: "INSERT OR IGNORE", columns: []string{"release_mbid", "artist_mbid", "artist_name", "join_phrase", "position"}},
		"release_labels":                {key: "release_labels", table: "release_labels", insertOp: "INSERT OR IGNORE", columns: []string{"release_mbid", "label_mbid", "label_name", "catalog_number"}},
		"release_media":                 {key: "release_media", table: "release_media", insertOp: "INSERT OR IGNORE", columns: []string{"release_mbid", "position", "format", "track_count"}},
		"recordings":                    {key: "recordings", table: "recordings", insertOp: "INSERT OR IGNORE", columns: []string{"mbid", "title", "length", "disambiguation", "video", "first_release_date"}},
		"recording_artists":             {key: "recording_artists", table: "recording_artists", insertOp: "INSERT OR IGNORE", columns: []string{"recording_mbid", "artist_mbid", "artist_name", "join_phrase", "position"}},
		"recording_isrcs":               {key: "recording_isrcs", table: "recording_isrcs", insertOp: "INSERT OR IGNORE", columns: []string{"recording_mbid", "isrc"}},
		"recording_tags":                {key: "recording_tags", table: "recording_tags", insertOp: "INSERT OR IGNORE", columns: []string{"recording_mbid", "tag", "count"}},
		"tracks":                        {key: "tracks", table: "tracks", insertOp: "INSERT OR IGNORE", columns: []string{"mbid", "release_mbid", "recording_mbid", "media_position", "position", "number", "title", "length"}},
		"external_links":                {key: "external_links", table: "external_links", insertOp: "INSERT OR IGNORE", columns: []string{"entity_type", "entity_mbid", "rel_type", "url"}},
	}
}

func (w *Writer) addRows(key string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	batch := w.batches[key]
	batch.rows = append(batch.rows, rows...)
	if len(batch.rows) >= w.batchSize {
		return w.flushBatch(batch)
	}
	return nil
}

func (w *Writer) flushBatch(batch *tableBatch) error {
	if batch == nil || len(batch.rows) == 0 {
		return nil
	}
	if w.tx == nil {
		return fmt.Errorf("writer transaction already closed")
	}
	sqlText, args := buildInsertSQL(batch.spec, batch.rows)
	if _, err := w.tx.ExecContext(w.ctx, sqlText, args...); err != nil {
		return fmt.Errorf("insert into %s: %w", batch.spec.table, err)
	}
	batch.rows = batch.rows[:0]
	return nil
}

func buildInsertSQL(spec tableSpec, rows [][]any) (string, []any) {
	var b strings.Builder
	b.WriteString(spec.insertOp)
	b.WriteString(" INTO ")
	b.WriteString(spec.table)
	b.WriteString(" (")
	b.WriteString(strings.Join(spec.columns, ", "))
	b.WriteString(") VALUES ")

	args := make([]any, 0, len(rows)*len(spec.columns))
	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for j := range spec.columns {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteByte('?')
		}
		b.WriteByte(')')
		args = append(args, row...)
	}
	return b.String(), args
}

func (w *Writer) addArtistRows(rows []model.ArtistRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.MBID, row.Name, row.SortName, row.Disambiguation, nullIfEmpty(row.Type), nullIfEmpty(row.Country), nullIfEmpty(row.Gender), nullIfEmpty(row.BeginDate), nullIfEmpty(row.EndDate), boolInt(row.Ended), nullIfEmpty(row.AreaMBID), nullIfEmpty(row.AreaName)})
	}
	return w.addRows("artists", vals)
}

func (w *Writer) addArtistAliasRows(rows []model.ArtistAliasRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ArtistMBID, row.Name, nullIfEmpty(row.SortName), nullIfEmpty(row.Type), row.Locale, boolInt(row.IsPrimary)})
	}
	return w.addRows("artist_aliases", vals)
}

func (w *Writer) addArtistTagRows(rows []model.ArtistTagRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ArtistMBID, row.Tag, row.Count})
	}
	return w.addRows("artist_tags", vals)
}

func (w *Writer) addArtistGenreRows(rows []model.ArtistGenreRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ArtistMBID, row.GenreMBID, row.GenreName, row.Count})
	}
	return w.addRows("artist_genres", vals)
}

func (w *Writer) addReleaseGroupRows(rows []model.ReleaseGroupRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.MBID, row.Title, nullIfEmpty(row.PrimaryType), row.Disambiguation, nullIfEmpty(row.FirstReleaseDate)})
	}
	return w.addRows("release_groups", vals)
}

func (w *Writer) addReleaseGroupSecondaryTypeRows(rows []model.ReleaseGroupSecondaryTypeRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ReleaseGroupMBID, row.Type})
	}
	return w.addRows("release_group_secondary_types", vals)
}

func (w *Writer) addReleaseGroupArtistRows(rows []model.ReleaseGroupArtistRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ReleaseGroupMBID, row.ArtistMBID, row.ArtistName, row.JoinPhrase, row.Position})
	}
	return w.addRows("release_group_artists", vals)
}

func (w *Writer) addReleaseGroupTagRows(rows []model.ReleaseGroupTagRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ReleaseGroupMBID, row.Tag, row.Count})
	}
	return w.addRows("release_group_tags", vals)
}

func (w *Writer) addReleaseRows(rows []model.ReleaseRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.MBID, row.Title, nullIfEmpty(row.Status), nullIfEmpty(row.Date), nullIfEmpty(row.Country), nullIfEmpty(row.Barcode), nullIfEmpty(row.Packaging), nullIfEmpty(row.Language), nullIfEmpty(row.Script), nullIfEmpty(row.ReleaseGroupMBID)})
	}
	return w.addRows("releases", vals)
}

func (w *Writer) addReleaseArtistRows(rows []model.ReleaseArtistRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ReleaseMBID, row.ArtistMBID, row.ArtistName, row.JoinPhrase, row.Position})
	}
	return w.addRows("release_artists", vals)
}

func (w *Writer) addReleaseLabelRows(rows []model.ReleaseLabelRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ReleaseMBID, row.LabelMBID, row.LabelName, row.CatalogNumber})
	}
	return w.addRows("release_labels", vals)
}

func (w *Writer) addReleaseMediaRows(rows []model.ReleaseMediaRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.ReleaseMBID, row.Position, nullIfEmpty(row.Format), row.TrackCount})
	}
	return w.addRows("release_media", vals)
}

func (w *Writer) addRecordingRows(rows []model.RecordingRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.MBID, row.Title, row.Length, row.Disambiguation, boolInt(row.Video), nullIfEmpty(row.FirstReleaseDate)})
	}
	return w.addRows("recordings", vals)
}

func (w *Writer) addRecordingArtistRows(rows []model.RecordingArtistRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.RecordingMBID, row.ArtistMBID, row.ArtistName, row.JoinPhrase, row.Position})
	}
	return w.addRows("recording_artists", vals)
}

func (w *Writer) addRecordingISRCRows(rows []model.RecordingISRCRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.RecordingMBID, row.ISRC})
	}
	return w.addRows("recording_isrcs", vals)
}

func (w *Writer) addRecordingTagRows(rows []model.RecordingTagRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.RecordingMBID, row.Tag, row.Count})
	}
	return w.addRows("recording_tags", vals)
}

func (w *Writer) addTrackRows(rows []model.TrackRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.MBID, row.ReleaseMBID, row.RecordingMBID, row.MediaPosition, row.Position, row.Number, row.Title, row.Length})
	}
	return w.addRows("tracks", vals)
}

func (w *Writer) addExternalLinkRows(rows []model.ExternalLinkRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, []any{row.EntityType, row.EntityMBID, row.RelType, row.URL})
	}
	return w.addRows("external_links", vals)
}

func nullIfEmpty(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
