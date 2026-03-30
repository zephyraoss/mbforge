package cli

import (
	"context"
	"testing"

	mbdb "github.com/zephyraoss/mbforge/internal/db"
	"github.com/zephyraoss/mbforge/internal/libsqlutil"
)

func TestSearchFindsAliasReleaseAndTrack(t *testing.T) {
	ctx := context.Background()

	db, err := libsqlutil.OpenLocal(":memory:")
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	defer db.Close()

	if err := mbdb.CreateSchema(ctx, db); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
	if err := mbdb.CreateIndexes(ctx, db); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}

	stmts := []string{
		`INSERT INTO artists(mbid, name, sort_name, type, country) VALUES('artist-1', 'Nine Inch Nails', 'Nine Inch Nails', 'Group', 'US')`,
		`INSERT INTO artist_aliases(artist_mbid, name, locale, is_primary) VALUES('artist-1', 'NIN', '', 1)`,
		`INSERT INTO release_groups(mbid, title, primary_type, first_release_date) VALUES('rg-1', 'The Downward Spiral', 'Album', '1994-03-08')`,
		`INSERT INTO release_group_artists(release_group_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('rg-1', 'artist-1', 'Nine Inch Nails', '', 1)`,
		`INSERT INTO releases(mbid, title, date, country, barcode, release_group_mbid) VALUES('release-1', 'The Downward Spiral', '1994-03-08', 'US', '123456789012', 'rg-1')`,
		`INSERT INTO release_artists(release_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('release-1', 'artist-1', 'Nine Inch Nails', '', 1)`,
		`INSERT INTO recordings(mbid, title, first_release_date) VALUES('recording-1', 'Closer', '1994-03-08')`,
		`INSERT INTO recording_artists(recording_mbid, artist_mbid, artist_name, join_phrase, position) VALUES('recording-1', 'artist-1', 'Nine Inch Nails', '', 1)`,
		`INSERT INTO tracks(mbid, release_mbid, recording_mbid, media_position, position, number, title) VALUES('track-1', 'release-1', 'recording-1', 1, 1, '1', 'Closer To God')`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	artists, err := searchArtists(ctx, db, "NIN", 10)
	if err != nil {
		t.Fatalf("searchArtists: %v", err)
	}
	if len(artists) != 1 || artists[0].MBID != "artist-1" {
		t.Fatalf("unexpected artist results: %+v", artists)
	}

	releases, err := searchReleases(ctx, db, "Downward", 10)
	if err != nil {
		t.Fatalf("searchReleases: %v", err)
	}
	if len(releases) != 1 || releases[0].MBID != "release-1" {
		t.Fatalf("unexpected release results: %+v", releases)
	}

	tracks, err := searchTracks(ctx, db, "Closer To God", 10)
	if err != nil {
		t.Fatalf("searchTracks: %v", err)
	}
	if len(tracks) != 1 || tracks[0].MBID != "track-1" {
		t.Fatalf("unexpected track results: %+v", tracks)
	}
}
