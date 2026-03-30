package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	mbdb "github.com/zephyraoss/mbforge/internal/db"
	"github.com/zephyraoss/mbforge/internal/libsqlutil"
)

var searchTokenRE = regexp.MustCompile(`[\p{L}\p{N}]+`)

type searchConfig struct {
	DBPath string
	Limit  int
}

type artistSearchResult struct {
	MBID     string
	Name     string
	SortName string
	Type     string
	Country  string
}

type releaseGroupSearchResult struct {
	MBID             string
	Title            string
	PrimaryType      string
	FirstReleaseDate string
	Artists          string
}

type releaseSearchResult struct {
	MBID    string
	Title   string
	Date    string
	Country string
	Artists string
}

type recordingSearchResult struct {
	MBID             string
	Title            string
	FirstReleaseDate string
	Artists          string
}

type trackSearchResult struct {
	MBID         string
	Title        string
	Number       string
	ReleaseTitle string
	Artists      string
}

type indexedSearchResult struct {
	EntityType string
	EntityMBID string
	Heading    string
	Subtitle   string
	Meta       string
	Aux        string
	Score      float64
}

func newSearchCmd() *cobra.Command {
	cfg := searchConfig{
		DBPath: "./musicbrainz.db",
		Limit:  10,
	}

	cmd := &cobra.Command{
		Use:   "search [query]",
		Short: "Search artists, recordings, releases, release groups, and tracks",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.TrimSpace(strings.Join(args, " "))
			return runSearch(cmd.Context(), cfg, query)
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Path to the database")
	cmd.Flags().IntVar(&cfg.Limit, "limit", cfg.Limit, "Maximum results per entity section")
	return cmd
}

func runSearch(ctx context.Context, cfg searchConfig, query string) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query is required")
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 10
	}

	db, err := libsqlutil.OpenLocal(cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := ensureMBForgeSchema(ctx, db); err != nil {
		return err
	}

	hasSearchIndex, err := mbdb.SearchIndexExists(ctx, db)
	if err != nil {
		return err
	}
	if hasSearchIndex {
		results, err := searchFast(ctx, db, query, cfg.Limit)
		if err != nil {
			return err
		}
		if len(results) == 0 {
			fmt.Fprintln(os.Stdout, "no matches")
			return nil
		}
		printIndexedResults(results)
		return nil
	}

	fmt.Fprintln(os.Stderr, "search index not found; run `mbforge search-index --db ./musicbrainz.db` for fast search. Falling back to slow SQL.")

	artists, err := searchArtists(ctx, db, query, cfg.Limit)
	if err != nil {
		return err
	}
	releaseGroups, err := searchReleaseGroups(ctx, db, query, cfg.Limit)
	if err != nil {
		return err
	}
	releases, err := searchReleases(ctx, db, query, cfg.Limit)
	if err != nil {
		return err
	}
	recordings, err := searchRecordings(ctx, db, query, cfg.Limit)
	if err != nil {
		return err
	}
	tracks, err := searchTracks(ctx, db, query, cfg.Limit)
	if err != nil {
		return err
	}

	if len(artists) == 0 && len(releaseGroups) == 0 && len(releases) == 0 && len(recordings) == 0 && len(tracks) == 0 {
		fmt.Fprintln(os.Stdout, "no matches")
		return nil
	}

	printArtistResults(artists)
	printReleaseGroupResults(releaseGroups)
	printReleaseResults(releases)
	printRecordingResults(recordings)
	printTrackResults(tracks)
	return nil
}

func searchFast(ctx context.Context, db *sql.DB, query string, limit int) ([]indexedSearchResult, error) {
	matchQuery, ok := buildFTSQuery(query)
	if !ok {
		return nil, nil
	}

	fetchLimit := limit * 8
	if fetchLimit < 50 {
		fetchLimit = 50
	}

	rows, err := db.QueryContext(ctx, `
SELECT
    entity_type,
    entity_mbid,
    heading,
    subtitle,
    meta,
    aux,
    bm25(search_fts, 8.0, 4.0, 2.0, 1.0) AS score
FROM search_fts
WHERE search_fts MATCH ?
ORDER BY score, entity_type, heading
LIMIT ?`, matchQuery, fetchLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	typeLimit := map[string]int{
		"artist":        limit,
		"release_group": limit,
		"release":       limit,
		"recording":     limit,
		"track":         limit,
	}
	typeCounts := make(map[string]int, len(typeLimit))
	out := make([]indexedSearchResult, 0, fetchLimit)

	for rows.Next() {
		var item indexedSearchResult
		if err := rows.Scan(&item.EntityType, &item.EntityMBID, &item.Heading, &item.Subtitle, &item.Meta, &item.Aux, &item.Score); err != nil {
			return nil, err
		}
		if typeCounts[item.EntityType] >= typeLimit[item.EntityType] {
			continue
		}
		typeCounts[item.EntityType]++
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func buildFTSQuery(query string) (string, bool) {
	tokens := searchTokenRE.FindAllString(strings.ToLower(query), -1)
	if len(tokens) == 0 {
		return "", false
	}

	parts := make([]string, 0, len(tokens))
	for _, token := range tokens {
		parts = append(parts, token+"*")
	}
	return strings.Join(parts, " AND "), true
}

func searchArtists(ctx context.Context, db *sql.DB, query string, limit int) ([]artistSearchResult, error) {
	exactSQL := `
SELECT a.mbid, a.name, COALESCE(a.sort_name, ''), COALESCE(a.type, ''), COALESCE(a.country, '')
FROM artists a
WHERE a.mbid = ?
   OR a.name LIKE ?
   OR a.sort_name LIKE ?
   OR EXISTS (
       SELECT 1 FROM artist_aliases aa
       WHERE aa.artist_mbid = a.mbid AND aa.name LIKE ?
   )
ORDER BY a.name
LIMIT ?`
	prefixSQL := `
SELECT a.mbid, a.name, COALESCE(a.sort_name, ''), COALESCE(a.type, ''), COALESCE(a.country, '')
FROM artists a
WHERE a.name LIKE ?
   OR a.sort_name LIKE ?
   OR EXISTS (
       SELECT 1 FROM artist_aliases aa
       WHERE aa.artist_mbid = a.mbid AND aa.name LIKE ?
   )
ORDER BY a.name
LIMIT ?`
	substrSQL := `
SELECT a.mbid, a.name, COALESCE(a.sort_name, ''), COALESCE(a.type, ''), COALESCE(a.country, '')
FROM artists a
WHERE a.name LIKE ?
   OR a.sort_name LIKE ?
   OR EXISTS (
       SELECT 1 FROM artist_aliases aa
       WHERE aa.artist_mbid = a.mbid AND aa.name LIKE ?
   )
ORDER BY a.name
LIMIT ?`

	collector := newCollector[artistSearchResult](limit, func(v artistSearchResult) string { return v.MBID })
	if err := collectArtistRows(ctx, db, exactSQL, collector, query, query, query, query, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	prefix := query + "%"
	if err := collectArtistRows(ctx, db, prefixSQL, collector, prefix, prefix, prefix, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	substr := "%" + query + "%"
	if err := collectArtistRows(ctx, db, substrSQL, collector, substr, substr, substr, limit); err != nil {
		return nil, err
	}
	return collector.Results(), nil
}

func collectArtistRows(ctx context.Context, db *sql.DB, query string, collector *collector[artistSearchResult], args ...any) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var item artistSearchResult
		if err := rows.Scan(&item.MBID, &item.Name, &item.SortName, &item.Type, &item.Country); err != nil {
			return err
		}
		collector.Add(item)
		if collector.Full() {
			return nil
		}
	}
	return rows.Err()
}

func searchReleaseGroups(ctx context.Context, db *sql.DB, query string, limit int) ([]releaseGroupSearchResult, error) {
	selectSQL := `
SELECT
    rg.mbid,
    rg.title,
    COALESCE(rg.primary_type, ''),
    COALESCE(rg.first_release_date, ''),
    COALESCE((
        SELECT group_concat(piece, '')
        FROM (
            SELECT rga.artist_name || rga.join_phrase AS piece
            FROM release_group_artists rga
            WHERE rga.release_group_mbid = rg.mbid
            ORDER BY rga.position
        )
    ), '')
FROM release_groups rg
WHERE %s
ORDER BY rg.first_release_date DESC, rg.title
LIMIT ?`

	collector := newCollector[releaseGroupSearchResult](limit, func(v releaseGroupSearchResult) string { return v.MBID })
	if err := collectReleaseGroupRows(ctx, db, fmt.Sprintf(selectSQL, `
rg.mbid = ?
OR rg.title LIKE ?
OR EXISTS (
    SELECT 1 FROM release_group_artists rga
    WHERE rga.release_group_mbid = rg.mbid AND rga.artist_name LIKE ?
)`), collector, query, query, query, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	prefix := query + "%"
	if err := collectReleaseGroupRows(ctx, db, fmt.Sprintf(selectSQL, `
rg.title LIKE ?
OR EXISTS (
    SELECT 1 FROM release_group_artists rga
    WHERE rga.release_group_mbid = rg.mbid AND rga.artist_name LIKE ?
)`), collector, prefix, prefix, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	substr := "%" + query + "%"
	if err := collectReleaseGroupRows(ctx, db, fmt.Sprintf(selectSQL, `
rg.title LIKE ?
OR EXISTS (
    SELECT 1 FROM release_group_artists rga
    WHERE rga.release_group_mbid = rg.mbid AND rga.artist_name LIKE ?
)`), collector, substr, substr, limit); err != nil {
		return nil, err
	}
	return collector.Results(), nil
}

func collectReleaseGroupRows(ctx context.Context, db *sql.DB, query string, collector *collector[releaseGroupSearchResult], args ...any) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var item releaseGroupSearchResult
		if err := rows.Scan(&item.MBID, &item.Title, &item.PrimaryType, &item.FirstReleaseDate, &item.Artists); err != nil {
			return err
		}
		collector.Add(item)
		if collector.Full() {
			return nil
		}
	}
	return rows.Err()
}

func searchReleases(ctx context.Context, db *sql.DB, query string, limit int) ([]releaseSearchResult, error) {
	selectSQL := `
SELECT
    r.mbid,
    r.title,
    COALESCE(r.date, ''),
    COALESCE(r.country, ''),
    COALESCE((
        SELECT group_concat(piece, '')
        FROM (
            SELECT ra.artist_name || ra.join_phrase AS piece
            FROM release_artists ra
            WHERE ra.release_mbid = r.mbid
            ORDER BY ra.position
        )
    ), '')
FROM releases r
WHERE %s
ORDER BY r.date DESC, r.title
LIMIT ?`

	collector := newCollector[releaseSearchResult](limit, func(v releaseSearchResult) string { return v.MBID })
	if err := collectReleaseRows(ctx, db, fmt.Sprintf(selectSQL, `
r.mbid = ?
OR r.title LIKE ?
OR r.barcode = ?
OR EXISTS (
    SELECT 1 FROM release_artists ra
    WHERE ra.release_mbid = r.mbid AND ra.artist_name LIKE ?
)
OR EXISTS (
    SELECT 1 FROM release_labels rl
    WHERE rl.release_mbid = r.mbid AND rl.label_name LIKE ?
)`), collector, query, query, query, query, query, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	prefix := query + "%"
	if err := collectReleaseRows(ctx, db, fmt.Sprintf(selectSQL, `
r.title LIKE ?
OR EXISTS (
    SELECT 1 FROM release_artists ra
    WHERE ra.release_mbid = r.mbid AND ra.artist_name LIKE ?
)
OR EXISTS (
    SELECT 1 FROM release_labels rl
    WHERE rl.release_mbid = r.mbid AND rl.label_name LIKE ?
)`), collector, prefix, prefix, prefix, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	substr := "%" + query + "%"
	if err := collectReleaseRows(ctx, db, fmt.Sprintf(selectSQL, `
r.title LIKE ?
OR EXISTS (
    SELECT 1 FROM release_artists ra
    WHERE ra.release_mbid = r.mbid AND ra.artist_name LIKE ?
)
OR EXISTS (
    SELECT 1 FROM release_labels rl
    WHERE rl.release_mbid = r.mbid AND rl.label_name LIKE ?
)`), collector, substr, substr, substr, limit); err != nil {
		return nil, err
	}
	return collector.Results(), nil
}

func collectReleaseRows(ctx context.Context, db *sql.DB, query string, collector *collector[releaseSearchResult], args ...any) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var item releaseSearchResult
		if err := rows.Scan(&item.MBID, &item.Title, &item.Date, &item.Country, &item.Artists); err != nil {
			return err
		}
		collector.Add(item)
		if collector.Full() {
			return nil
		}
	}
	return rows.Err()
}

func searchRecordings(ctx context.Context, db *sql.DB, query string, limit int) ([]recordingSearchResult, error) {
	selectSQL := `
SELECT
    r.mbid,
    r.title,
    COALESCE(r.first_release_date, ''),
    COALESCE((
        SELECT group_concat(piece, '')
        FROM (
            SELECT ra.artist_name || ra.join_phrase AS piece
            FROM recording_artists ra
            WHERE ra.recording_mbid = r.mbid
            ORDER BY ra.position
        )
    ), '')
FROM recordings r
WHERE %s
ORDER BY r.first_release_date DESC, r.title
LIMIT ?`

	collector := newCollector[recordingSearchResult](limit, func(v recordingSearchResult) string { return v.MBID })
	if err := collectRecordingRows(ctx, db, fmt.Sprintf(selectSQL, `
r.mbid = ?
OR r.title LIKE ?
OR EXISTS (
    SELECT 1 FROM recording_artists ra
    WHERE ra.recording_mbid = r.mbid AND ra.artist_name LIKE ?
)
OR EXISTS (
    SELECT 1 FROM recording_isrcs ri
    WHERE ri.recording_mbid = r.mbid AND ri.isrc = ?
)
OR EXISTS (
    SELECT 1 FROM tracks t
    WHERE t.recording_mbid = r.mbid AND t.title LIKE ?
)`), collector, query, query, query, query, query, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	prefix := query + "%"
	if err := collectRecordingRows(ctx, db, fmt.Sprintf(selectSQL, `
r.title LIKE ?
OR EXISTS (
    SELECT 1 FROM recording_artists ra
    WHERE ra.recording_mbid = r.mbid AND ra.artist_name LIKE ?
)
OR EXISTS (
    SELECT 1 FROM tracks t
    WHERE t.recording_mbid = r.mbid AND t.title LIKE ?
)`), collector, prefix, prefix, prefix, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	substr := "%" + query + "%"
	if err := collectRecordingRows(ctx, db, fmt.Sprintf(selectSQL, `
r.title LIKE ?
OR EXISTS (
    SELECT 1 FROM recording_artists ra
    WHERE ra.recording_mbid = r.mbid AND ra.artist_name LIKE ?
)
OR EXISTS (
    SELECT 1 FROM tracks t
    WHERE t.recording_mbid = r.mbid AND t.title LIKE ?
)`), collector, substr, substr, substr, limit); err != nil {
		return nil, err
	}
	return collector.Results(), nil
}

func collectRecordingRows(ctx context.Context, db *sql.DB, query string, collector *collector[recordingSearchResult], args ...any) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var item recordingSearchResult
		if err := rows.Scan(&item.MBID, &item.Title, &item.FirstReleaseDate, &item.Artists); err != nil {
			return err
		}
		collector.Add(item)
		if collector.Full() {
			return nil
		}
	}
	return rows.Err()
}

func searchTracks(ctx context.Context, db *sql.DB, query string, limit int) ([]trackSearchResult, error) {
	selectSQL := `
SELECT
    t.mbid,
    t.title,
    t.number,
    COALESCE(r.title, ''),
    COALESCE((
        SELECT group_concat(piece, '')
        FROM (
            SELECT ra.artist_name || ra.join_phrase AS piece
            FROM release_artists ra
            WHERE ra.release_mbid = t.release_mbid
            ORDER BY ra.position
        )
    ), '')
FROM tracks t
JOIN releases r ON r.mbid = t.release_mbid
WHERE %s
ORDER BY r.date DESC, t.title
LIMIT ?`

	collector := newCollector[trackSearchResult](limit, func(v trackSearchResult) string { return v.MBID })
	if err := collectTrackRows(ctx, db, fmt.Sprintf(selectSQL, `
t.mbid = ?
OR t.title LIKE ?
OR EXISTS (
    SELECT 1 FROM release_artists ra
    WHERE ra.release_mbid = t.release_mbid AND ra.artist_name LIKE ?
)`), collector, query, query, query, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	prefix := query + "%"
	if err := collectTrackRows(ctx, db, fmt.Sprintf(selectSQL, `
t.title LIKE ?
OR EXISTS (
    SELECT 1 FROM release_artists ra
    WHERE ra.release_mbid = t.release_mbid AND ra.artist_name LIKE ?
)`), collector, prefix, prefix, limit); err != nil {
		return nil, err
	}
	if collector.Full() {
		return collector.Results(), nil
	}
	substr := "%" + query + "%"
	if err := collectTrackRows(ctx, db, fmt.Sprintf(selectSQL, `
t.title LIKE ?
OR EXISTS (
    SELECT 1 FROM release_artists ra
    WHERE ra.release_mbid = t.release_mbid AND ra.artist_name LIKE ?
)`), collector, substr, substr, limit); err != nil {
		return nil, err
	}
	return collector.Results(), nil
}

func collectTrackRows(ctx context.Context, db *sql.DB, query string, collector *collector[trackSearchResult], args ...any) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var item trackSearchResult
		if err := rows.Scan(&item.MBID, &item.Title, &item.Number, &item.ReleaseTitle, &item.Artists); err != nil {
			return err
		}
		collector.Add(item)
		if collector.Full() {
			return nil
		}
	}
	return rows.Err()
}

type collector[T any] struct {
	limit   int
	keyFn   func(T) string
	seen    map[string]struct{}
	results []T
}

func newCollector[T any](limit int, keyFn func(T) string) *collector[T] {
	return &collector[T]{
		limit:   limit,
		keyFn:   keyFn,
		seen:    make(map[string]struct{}),
		results: make([]T, 0, limit),
	}
}

func (c *collector[T]) Add(item T) {
	if c.Full() {
		return
	}
	key := c.keyFn(item)
	if _, ok := c.seen[key]; ok {
		return
	}
	c.seen[key] = struct{}{}
	c.results = append(c.results, item)
}

func (c *collector[T]) Full() bool {
	return len(c.results) >= c.limit
}

func (c *collector[T]) Results() []T {
	return c.results
}

func printArtistResults(results []artistSearchResult) {
	if len(results) == 0 {
		return
	}
	fmt.Fprintln(os.Stdout, "Artists")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "MBID\tName\tSort Name\tType\tCountry")
	for _, item := range results {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", item.MBID, item.Name, item.SortName, item.Type, item.Country)
	}
	_ = w.Flush()
	fmt.Fprintln(os.Stdout)
}

func printReleaseGroupResults(results []releaseGroupSearchResult) {
	if len(results) == 0 {
		return
	}
	fmt.Fprintln(os.Stdout, "Release Groups")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "MBID\tTitle\tArtists\tPrimary Type\tFirst Release")
	for _, item := range results {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", item.MBID, item.Title, item.Artists, item.PrimaryType, item.FirstReleaseDate)
	}
	_ = w.Flush()
	fmt.Fprintln(os.Stdout)
}

func printReleaseResults(results []releaseSearchResult) {
	if len(results) == 0 {
		return
	}
	fmt.Fprintln(os.Stdout, "Releases")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "MBID\tTitle\tArtists\tDate\tCountry")
	for _, item := range results {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", item.MBID, item.Title, item.Artists, item.Date, item.Country)
	}
	_ = w.Flush()
	fmt.Fprintln(os.Stdout)
}

func printRecordingResults(results []recordingSearchResult) {
	if len(results) == 0 {
		return
	}
	fmt.Fprintln(os.Stdout, "Recordings")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "MBID\tTitle\tArtists\tFirst Release")
	for _, item := range results {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", item.MBID, item.Title, item.Artists, item.FirstReleaseDate)
	}
	_ = w.Flush()
	fmt.Fprintln(os.Stdout)
}

func printTrackResults(results []trackSearchResult) {
	if len(results) == 0 {
		return
	}
	fmt.Fprintln(os.Stdout, "Tracks")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "MBID\tTitle\tNumber\tRelease\tArtists")
	for _, item := range results {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", item.MBID, item.Title, item.Number, item.ReleaseTitle, item.Artists)
	}
	_ = w.Flush()
}

func printIndexedResults(results []indexedSearchResult) {
	orderedTypes := []string{"artist", "release_group", "release", "recording", "track"}
	sectionTitles := map[string]string{
		"artist":        "Artists",
		"release_group": "Release Groups",
		"release":       "Releases",
		"recording":     "Recordings",
		"track":         "Tracks",
	}

	grouped := make(map[string][]indexedSearchResult, len(orderedTypes))
	for _, item := range results {
		grouped[item.EntityType] = append(grouped[item.EntityType], item)
	}

	firstSection := true
	for _, entityType := range orderedTypes {
		items := grouped[entityType]
		if len(items) == 0 {
			continue
		}
		if !firstSection {
			fmt.Fprintln(os.Stdout)
		}
		firstSection = false

		fmt.Fprintln(os.Stdout, sectionTitles[entityType])
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "MBID\tTitle\tContext\tMeta")
		for _, item := range items {
			context := item.Subtitle
			if item.Aux != "" {
				if context != "" {
					context += " | " + item.Aux
				} else {
					context = item.Aux
				}
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", item.EntityMBID, item.Heading, context, item.Meta)
		}
		_ = w.Flush()
	}
}
