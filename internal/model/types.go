package model

type DumpMetadata struct {
	DumpDir             string
	ReplicationSequence string
	SchemaSequence      string
	JSONSchemaNumber    string
	DumpTimestamp       string
}

type ArtistRow struct {
	MBID           string
	Name           string
	SortName       string
	Disambiguation string
	Type           string
	Country        string
	Gender         string
	BeginDate      string
	EndDate        string
	Ended          bool
	AreaMBID       string
	AreaName       string
}

type ArtistAliasRow struct {
	ArtistMBID string
	Name       string
	SortName   string
	Type       string
	Locale     string
	IsPrimary  bool
}

type ArtistTagRow struct {
	ArtistMBID string
	Tag        string
	Count      int
}

type ArtistGenreRow struct {
	ArtistMBID string
	GenreMBID  string
	GenreName  string
	Count      int
}

type ReleaseGroupRow struct {
	MBID             string
	Title            string
	PrimaryType      string
	Disambiguation   string
	FirstReleaseDate string
}

type ReleaseGroupSecondaryTypeRow struct {
	ReleaseGroupMBID string
	Type             string
}

type ReleaseGroupArtistRow struct {
	ReleaseGroupMBID string
	ArtistMBID       string
	ArtistName       string
	JoinPhrase       string
	Position         int
}

type ReleaseGroupTagRow struct {
	ReleaseGroupMBID string
	Tag              string
	Count            int
}

type ReleaseRow struct {
	MBID             string
	Title            string
	Status           string
	Date             string
	Country          string
	Barcode          string
	Packaging        string
	Language         string
	Script           string
	ReleaseGroupMBID string
}

type ReleaseArtistRow struct {
	ReleaseMBID string
	ArtistMBID  string
	ArtistName  string
	JoinPhrase  string
	Position    int
}

type ReleaseLabelRow struct {
	ReleaseMBID   string
	LabelMBID     string
	LabelName     string
	CatalogNumber string
}

type ReleaseMediaRow struct {
	ReleaseMBID string
	Position    int
	Format      string
	TrackCount  int
}

type RecordingRow struct {
	MBID             string
	Title            string
	Length           *int
	Disambiguation   string
	Video            bool
	FirstReleaseDate string
}

type RecordingArtistRow struct {
	RecordingMBID string
	ArtistMBID    string
	ArtistName    string
	JoinPhrase    string
	Position      int
}

type RecordingISRCRow struct {
	RecordingMBID string
	ISRC          string
}

type RecordingTagRow struct {
	RecordingMBID string
	Tag           string
	Count         int
}

type TrackRow struct {
	MBID          string
	ReleaseMBID   string
	RecordingMBID string
	MediaPosition int
	Position      int
	Number        string
	Title         string
	Length        *int
}

type ExternalLinkRow struct {
	EntityType string
	EntityMBID string
	RelType    string
	URL        string
}

type Mutation struct {
	Artists                    []ArtistRow
	ArtistAliases              []ArtistAliasRow
	ArtistTags                 []ArtistTagRow
	ArtistGenres               []ArtistGenreRow
	ReleaseGroups              []ReleaseGroupRow
	ReleaseGroupSecondaryTypes []ReleaseGroupSecondaryTypeRow
	ReleaseGroupArtists        []ReleaseGroupArtistRow
	ReleaseGroupTags           []ReleaseGroupTagRow
	Releases                   []ReleaseRow
	ReleaseArtists             []ReleaseArtistRow
	ReleaseLabels              []ReleaseLabelRow
	ReleaseMedia               []ReleaseMediaRow
	Recordings                 []RecordingRow
	RecordingArtists           []RecordingArtistRow
	RecordingISRCs             []RecordingISRCRow
	RecordingTags              []RecordingTagRow
	Tracks                     []TrackRow
	ExternalLinks              []ExternalLinkRow
}

func (m Mutation) Empty() bool {
	return len(m.Artists) == 0 &&
		len(m.ArtistAliases) == 0 &&
		len(m.ArtistTags) == 0 &&
		len(m.ArtistGenres) == 0 &&
		len(m.ReleaseGroups) == 0 &&
		len(m.ReleaseGroupSecondaryTypes) == 0 &&
		len(m.ReleaseGroupArtists) == 0 &&
		len(m.ReleaseGroupTags) == 0 &&
		len(m.Releases) == 0 &&
		len(m.ReleaseArtists) == 0 &&
		len(m.ReleaseLabels) == 0 &&
		len(m.ReleaseMedia) == 0 &&
		len(m.Recordings) == 0 &&
		len(m.RecordingArtists) == 0 &&
		len(m.RecordingISRCs) == 0 &&
		len(m.RecordingTags) == 0 &&
		len(m.Tracks) == 0 &&
		len(m.ExternalLinks) == 0
}
