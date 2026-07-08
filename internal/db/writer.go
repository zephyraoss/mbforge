package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/zephyraoss/mbforge/internal/model"
)

const maxSQLiteVariables = 32766

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
	if err := w.addArtistRelationshipRows(m.ArtistRelationships); err != nil {
		return err
	}
	if err := w.addLabelRows(m.Labels); err != nil {
		return err
	}
	if err := w.addLabelAliasRows(m.LabelAliases); err != nil {
		return err
	}
	if err := w.addLabelTagRows(m.LabelTags); err != nil {
		return err
	}
	if err := w.addLabelGenreRows(m.LabelGenres); err != nil {
		return err
	}
	if err := w.addWorkRows(m.Works); err != nil {
		return err
	}
	if err := w.addWorkAliasRows(m.WorkAliases); err != nil {
		return err
	}
	if err := w.addWorkISWCRows(m.WorkISWCs); err != nil {
		return err
	}
	if err := w.addWorkTagRows(m.WorkTags); err != nil {
		return err
	}
	if err := w.addRecordingWorkRows(m.RecordingWorks); err != nil {
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
		"artist_relationships":          {key: "artist_relationships", table: "artist_relationships", insertOp: "INSERT OR IGNORE", columns: []string{"artist_mbid", "related_artist_mbid", "related_artist_name", "type", "direction", "begin_date", "end_date", "ended", "attributes"}},
		"labels":                        {key: "labels", table: "labels", insertOp: "INSERT", columns: []string{"mbid", "name", "sort_name", "disambiguation", "type", "label_code", "country", "begin_date", "end_date", "ended", "area_mbid", "area_name"}},
		"label_aliases":                 {key: "label_aliases", table: "label_aliases", insertOp: "INSERT OR IGNORE", columns: []string{"label_mbid", "name", "sort_name", "type", "locale", "is_primary"}},
		"label_tags":                    {key: "label_tags", table: "label_tags", insertOp: "INSERT OR IGNORE", columns: []string{"label_mbid", "tag", "count"}},
		"label_genres":                  {key: "label_genres", table: "label_genres", insertOp: "INSERT OR IGNORE", columns: []string{"label_mbid", "genre_mbid", "genre_name", "count"}},
		"works":                         {key: "works", table: "works", insertOp: "INSERT OR IGNORE", columns: []string{"mbid", "title", "disambiguation", "type", "languages"}},
		"work_aliases":                  {key: "work_aliases", table: "work_aliases", insertOp: "INSERT OR IGNORE", columns: []string{"work_mbid", "name", "sort_name", "type", "locale", "is_primary"}},
		"work_iswcs":                    {key: "work_iswcs", table: "work_iswcs", insertOp: "INSERT OR IGNORE", columns: []string{"work_mbid", "iswc"}},
		"work_tags":                     {key: "work_tags", table: "work_tags", insertOp: "INSERT OR IGNORE", columns: []string{"work_mbid", "tag", "count"}},
		"recording_works":               {key: "recording_works", table: "recording_works", insertOp: "INSERT OR IGNORE", columns: []string{"recording_mbid", "work_mbid", "type", "attributes"}},
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

	maxRows := maxRowsPerInsert(batch.spec)
	for start := 0; start < len(batch.rows); start += maxRows {
		end := start + maxRows
		if end > len(batch.rows) {
			end = len(batch.rows)
		}
		sqlText, args := buildInsertSQL(batch.spec, batch.rows[start:end])
		if _, err := w.tx.ExecContext(w.ctx, sqlText, args...); err != nil {
			return fmt.Errorf("insert into %s: %w", batch.spec.table, err)
		}
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

func maxRowsPerInsert(spec tableSpec) int {
	columnCount := len(spec.columns)
	if columnCount <= 0 {
		return 1
	}
	maxRows := maxSQLiteVariables / columnCount
	if maxRows < 1 {
		return 1
	}
	return maxRows
}

func (w *Writer) addArtistRows(rows []model.ArtistRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, artistRowValues(row))
	}
	return w.addRows("artists", vals)
}

func (w *Writer) addArtistAliasRows(rows []model.ArtistAliasRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, artistAliasRowValues(row))
	}
	return w.addRows("artist_aliases", vals)
}

func (w *Writer) addArtistTagRows(rows []model.ArtistTagRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, artistTagRowValues(row))
	}
	return w.addRows("artist_tags", vals)
}

func (w *Writer) addArtistGenreRows(rows []model.ArtistGenreRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, artistGenreRowValues(row))
	}
	return w.addRows("artist_genres", vals)
}

func (w *Writer) addArtistRelationshipRows(rows []model.ArtistRelationshipRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, artistRelationshipRowValues(row))
	}
	return w.addRows("artist_relationships", vals)
}

func (w *Writer) addLabelRows(rows []model.LabelRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, labelRowValues(row))
	}
	return w.addRows("labels", vals)
}

func (w *Writer) addLabelAliasRows(rows []model.LabelAliasRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, labelAliasRowValues(row))
	}
	return w.addRows("label_aliases", vals)
}

func (w *Writer) addLabelTagRows(rows []model.LabelTagRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, labelTagRowValues(row))
	}
	return w.addRows("label_tags", vals)
}

func (w *Writer) addLabelGenreRows(rows []model.LabelGenreRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, labelGenreRowValues(row))
	}
	return w.addRows("label_genres", vals)
}

func (w *Writer) addWorkRows(rows []model.WorkRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, workRowValues(row))
	}
	return w.addRows("works", vals)
}

func (w *Writer) addWorkAliasRows(rows []model.WorkAliasRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, workAliasRowValues(row))
	}
	return w.addRows("work_aliases", vals)
}

func (w *Writer) addWorkISWCRows(rows []model.WorkISWCRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, workISWCRowValues(row))
	}
	return w.addRows("work_iswcs", vals)
}

func (w *Writer) addWorkTagRows(rows []model.WorkTagRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, workTagRowValues(row))
	}
	return w.addRows("work_tags", vals)
}

func (w *Writer) addRecordingWorkRows(rows []model.RecordingWorkRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, recordingWorkRowValues(row))
	}
	return w.addRows("recording_works", vals)
}

func (w *Writer) addReleaseGroupRows(rows []model.ReleaseGroupRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, releaseGroupRowValues(row))
	}
	return w.addRows("release_groups", vals)
}

func (w *Writer) addReleaseGroupSecondaryTypeRows(rows []model.ReleaseGroupSecondaryTypeRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, releaseGroupSecondaryTypeRowValues(row))
	}
	return w.addRows("release_group_secondary_types", vals)
}

func (w *Writer) addReleaseGroupArtistRows(rows []model.ReleaseGroupArtistRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, releaseGroupArtistRowValues(row))
	}
	return w.addRows("release_group_artists", vals)
}

func (w *Writer) addReleaseGroupTagRows(rows []model.ReleaseGroupTagRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, releaseGroupTagRowValues(row))
	}
	return w.addRows("release_group_tags", vals)
}

func (w *Writer) addReleaseRows(rows []model.ReleaseRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, releaseRowValues(row))
	}
	return w.addRows("releases", vals)
}

func (w *Writer) addReleaseArtistRows(rows []model.ReleaseArtistRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, releaseArtistRowValues(row))
	}
	return w.addRows("release_artists", vals)
}

func (w *Writer) addReleaseLabelRows(rows []model.ReleaseLabelRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, releaseLabelRowValues(row))
	}
	return w.addRows("release_labels", vals)
}

func (w *Writer) addReleaseMediaRows(rows []model.ReleaseMediaRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, releaseMediaRowValues(row))
	}
	return w.addRows("release_media", vals)
}

func (w *Writer) addRecordingRows(rows []model.RecordingRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, recordingRowValues(row))
	}
	return w.addRows("recordings", vals)
}

func (w *Writer) addRecordingArtistRows(rows []model.RecordingArtistRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, recordingArtistRowValues(row))
	}
	return w.addRows("recording_artists", vals)
}

func (w *Writer) addRecordingISRCRows(rows []model.RecordingISRCRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, recordingISRCRowValues(row))
	}
	return w.addRows("recording_isrcs", vals)
}

func (w *Writer) addRecordingTagRows(rows []model.RecordingTagRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, recordingTagRowValues(row))
	}
	return w.addRows("recording_tags", vals)
}

func (w *Writer) addTrackRows(rows []model.TrackRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, trackRowValues(row))
	}
	return w.addRows("tracks", vals)
}

func (w *Writer) addExternalLinkRows(rows []model.ExternalLinkRow) error {
	vals := make([][]any, 0, len(rows))
	for _, row := range rows {
		vals = append(vals, externalLinkRowValues(row))
	}
	return w.addRows("external_links", vals)
}

func artistRowValues(row model.ArtistRow) []any {
	return []any{row.MBID, row.Name, row.SortName, row.Disambiguation, nullIfEmpty(row.Type), nullIfEmpty(row.Country), nullIfEmpty(row.Gender), nullIfEmpty(row.BeginDate), nullIfEmpty(row.EndDate), boolInt(row.Ended), nullIfEmpty(row.AreaMBID), nullIfEmpty(row.AreaName)}
}

func artistAliasRowValues(row model.ArtistAliasRow) []any {
	return []any{row.ArtistMBID, row.Name, nullIfEmpty(row.SortName), nullIfEmpty(row.Type), row.Locale, boolInt(row.IsPrimary)}
}

func artistTagRowValues(row model.ArtistTagRow) []any {
	return []any{row.ArtistMBID, row.Tag, row.Count}
}

func artistGenreRowValues(row model.ArtistGenreRow) []any {
	return []any{row.ArtistMBID, row.GenreMBID, row.GenreName, row.Count}
}

func artistRelationshipRowValues(row model.ArtistRelationshipRow) []any {
	return []any{row.ArtistMBID, row.RelatedArtistMBID, row.RelatedArtistName, row.Type, row.Direction, row.BeginDate, row.EndDate, boolInt(row.Ended), row.Attributes}
}

func labelRowValues(row model.LabelRow) []any {
	return []any{row.MBID, row.Name, row.SortName, row.Disambiguation, nullIfEmpty(row.Type), row.LabelCode, nullIfEmpty(row.Country), nullIfEmpty(row.BeginDate), nullIfEmpty(row.EndDate), boolInt(row.Ended), nullIfEmpty(row.AreaMBID), nullIfEmpty(row.AreaName)}
}

func labelAliasRowValues(row model.LabelAliasRow) []any {
	return []any{row.LabelMBID, row.Name, nullIfEmpty(row.SortName), nullIfEmpty(row.Type), row.Locale, boolInt(row.IsPrimary)}
}

func labelTagRowValues(row model.LabelTagRow) []any {
	return []any{row.LabelMBID, row.Tag, row.Count}
}

func labelGenreRowValues(row model.LabelGenreRow) []any {
	return []any{row.LabelMBID, row.GenreMBID, row.GenreName, row.Count}
}

func workRowValues(row model.WorkRow) []any {
	return []any{row.MBID, row.Title, row.Disambiguation, nullIfEmpty(row.Type), row.Languages}
}

func workAliasRowValues(row model.WorkAliasRow) []any {
	return []any{row.WorkMBID, row.Name, nullIfEmpty(row.SortName), nullIfEmpty(row.Type), row.Locale, boolInt(row.IsPrimary)}
}

func workISWCRowValues(row model.WorkISWCRow) []any {
	return []any{row.WorkMBID, row.ISWC}
}

func workTagRowValues(row model.WorkTagRow) []any {
	return []any{row.WorkMBID, row.Tag, row.Count}
}

func recordingWorkRowValues(row model.RecordingWorkRow) []any {
	return []any{row.RecordingMBID, row.WorkMBID, row.Type, row.Attributes}
}

func releaseGroupRowValues(row model.ReleaseGroupRow) []any {
	return []any{row.MBID, row.Title, nullIfEmpty(row.PrimaryType), row.Disambiguation, nullIfEmpty(row.FirstReleaseDate)}
}

func releaseGroupSecondaryTypeRowValues(row model.ReleaseGroupSecondaryTypeRow) []any {
	return []any{row.ReleaseGroupMBID, row.Type}
}

func releaseGroupArtistRowValues(row model.ReleaseGroupArtistRow) []any {
	return []any{row.ReleaseGroupMBID, row.ArtistMBID, row.ArtistName, row.JoinPhrase, row.Position}
}

func releaseGroupTagRowValues(row model.ReleaseGroupTagRow) []any {
	return []any{row.ReleaseGroupMBID, row.Tag, row.Count}
}

func releaseRowValues(row model.ReleaseRow) []any {
	return []any{row.MBID, row.Title, nullIfEmpty(row.Status), nullIfEmpty(row.Date), nullIfEmpty(row.Country), nullIfEmpty(row.Barcode), nullIfEmpty(row.Packaging), nullIfEmpty(row.Language), nullIfEmpty(row.Script), nullIfEmpty(row.ReleaseGroupMBID)}
}

func releaseArtistRowValues(row model.ReleaseArtistRow) []any {
	return []any{row.ReleaseMBID, row.ArtistMBID, row.ArtistName, row.JoinPhrase, row.Position}
}

func releaseLabelRowValues(row model.ReleaseLabelRow) []any {
	return []any{row.ReleaseMBID, row.LabelMBID, row.LabelName, row.CatalogNumber}
}

func releaseMediaRowValues(row model.ReleaseMediaRow) []any {
	return []any{row.ReleaseMBID, row.Position, nullIfEmpty(row.Format), row.TrackCount}
}

func recordingRowValues(row model.RecordingRow) []any {
	return []any{row.MBID, row.Title, row.Length, row.Disambiguation, boolInt(row.Video), nullIfEmpty(row.FirstReleaseDate)}
}

func recordingArtistRowValues(row model.RecordingArtistRow) []any {
	return []any{row.RecordingMBID, row.ArtistMBID, row.ArtistName, row.JoinPhrase, row.Position}
}

func recordingISRCRowValues(row model.RecordingISRCRow) []any {
	return []any{row.RecordingMBID, row.ISRC}
}

func recordingTagRowValues(row model.RecordingTagRow) []any {
	return []any{row.RecordingMBID, row.Tag, row.Count}
}

func trackRowValues(row model.TrackRow) []any {
	return []any{row.MBID, row.ReleaseMBID, row.RecordingMBID, row.MediaPosition, row.Position, row.Number, row.Title, row.Length}
}

func externalLinkRowValues(row model.ExternalLinkRow) []any {
	return []any{row.EntityType, row.EntityMBID, row.RelType, row.URL}
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
