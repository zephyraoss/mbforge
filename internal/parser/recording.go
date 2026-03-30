package parser

import (
	"encoding/json"
	"fmt"

	"github.com/zephyraoss/mbforge/internal/model"
)

type recordingDoc struct {
	ID               string         `json:"id"`
	Title            string         `json:"title"`
	Length           *int           `json:"length"`
	Disambiguation   string         `json:"disambiguation"`
	Video            bool           `json:"video"`
	FirstReleaseDate string         `json:"first-release-date"`
	ArtistCredit     []artistCredit `json:"artist-credit"`
	ISRCs            []string       `json:"isrcs"`
	Tags             []tag          `json:"tags"`
	Relations        []relation     `json:"relations"`
}

func ParseRecording(line []byte) (model.Mutation, error) {
	var doc recordingDoc
	if err := json.Unmarshal(line, &doc); err != nil {
		return model.Mutation{}, err
	}
	mbid := normalizeString(doc.ID)
	title := normalizeString(doc.Title)
	if mbid == "" || title == "" {
		return model.Mutation{}, fmt.Errorf("recording missing id or title")
	}

	m := model.Mutation{}
	appendRecording(&m, &doc, true)
	return m, nil
}

func appendRecording(m *model.Mutation, doc *recordingDoc, includeTags bool) {
	if m == nil || doc == nil {
		return
	}
	mbid := normalizeString(doc.ID)
	title := normalizeString(doc.Title)
	if mbid == "" || title == "" {
		return
	}

	m.Recordings = append(m.Recordings, model.RecordingRow{
		MBID:             mbid,
		Title:            title,
		Length:           intPtr(doc.Length),
		Disambiguation:   normalizeString(doc.Disambiguation),
		Video:            doc.Video,
		FirstReleaseDate: normalizeString(doc.FirstReleaseDate),
	})
	m.RecordingArtists = append(m.RecordingArtists, normalizeArtistCreditForRecording(mbid, doc.ArtistCredit)...)
	m.ExternalLinks = append(m.ExternalLinks, normalizeExternalLinks("recording", mbid, doc.Relations)...)

	isrcSeen := make(map[string]struct{}, len(doc.ISRCs))
	for _, isrc := range doc.ISRCs {
		isrc = normalizeString(isrc)
		if isrc == "" {
			continue
		}
		if _, ok := isrcSeen[isrc]; ok {
			continue
		}
		isrcSeen[isrc] = struct{}{}
		m.RecordingISRCs = append(m.RecordingISRCs, model.RecordingISRCRow{
			RecordingMBID: mbid,
			ISRC:          isrc,
		})
	}

	if includeTags {
		m.RecordingTags = append(m.RecordingTags, normalizeTagRows(mbid, doc.Tags)...)
	}
}
