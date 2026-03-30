package parser

import (
	"encoding/json"
	"fmt"

	"github.com/zephyraoss/mbforge/internal/model"
)

type releaseDoc struct {
	ID                 string             `json:"id"`
	Title              string             `json:"title"`
	Status             string             `json:"status"`
	Date               string             `json:"date"`
	Country            string             `json:"country"`
	Barcode            string             `json:"barcode"`
	Packaging          string             `json:"packaging"`
	ReleaseGroup       *releaseGroupRef   `json:"release-group"`
	ArtistCredit       []artistCredit     `json:"artist-credit"`
	LabelInfo          []labelInfo        `json:"label-info"`
	TextRepresentation textRepresentation `json:"text-representation"`
	Media              []mediumDoc        `json:"media"`
	Relations          []relation         `json:"relations"`
}

type releaseGroupRef struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type labelInfo struct {
	CatalogNumber string    `json:"catalog-number"`
	Label         *labelRef `json:"label"`
}

type mediumDoc struct {
	Format     string     `json:"format"`
	Position   int        `json:"position"`
	TrackCount int        `json:"track-count"`
	Tracks     []trackDoc `json:"tracks"`
}

type trackDoc struct {
	ID        string        `json:"id"`
	Number    string        `json:"number"`
	Title     string        `json:"title"`
	Length    *int          `json:"length"`
	Position  int           `json:"position"`
	Recording *recordingDoc `json:"recording"`
}

func ParseRelease(line []byte) (model.Mutation, error) {
	var doc releaseDoc
	if err := json.Unmarshal(line, &doc); err != nil {
		return model.Mutation{}, err
	}
	mbid := normalizeString(doc.ID)
	title := normalizeString(doc.Title)
	if mbid == "" || title == "" {
		return model.Mutation{}, fmt.Errorf("release missing id or title")
	}

	m := model.Mutation{
		Releases: []model.ReleaseRow{{
			MBID:      mbid,
			Title:     title,
			Status:    normalizeString(doc.Status),
			Date:      normalizeString(doc.Date),
			Country:   normalizeString(doc.Country),
			Barcode:   normalizeString(doc.Barcode),
			Packaging: normalizeString(doc.Packaging),
			Language:  normalizeString(doc.TextRepresentation.Language),
			Script:    normalizeString(doc.TextRepresentation.Script),
		}},
		ReleaseArtists: normalizeArtistCreditForRelease(mbid, doc.ArtistCredit),
		ExternalLinks:  normalizeExternalLinks("release", mbid, doc.Relations),
	}

	if doc.ReleaseGroup != nil {
		m.Releases[0].ReleaseGroupMBID = normalizeString(doc.ReleaseGroup.ID)
	}

	labelSeen := make(map[string]struct{}, len(doc.LabelInfo))
	for _, item := range doc.LabelInfo {
		labelMBID, labelName := "", ""
		if item.Label != nil {
			labelMBID = normalizeString(item.Label.ID)
			labelName = normalizeString(item.Label.Name)
		}
		catalogNumber := normalizeString(item.CatalogNumber)
		key := labelMBID + "\x00" + catalogNumber
		if key == "\x00" {
			continue
		}
		if _, ok := labelSeen[key]; ok {
			continue
		}
		labelSeen[key] = struct{}{}
		m.ReleaseLabels = append(m.ReleaseLabels, model.ReleaseLabelRow{
			ReleaseMBID:   mbid,
			LabelMBID:     labelMBID,
			LabelName:     labelName,
			CatalogNumber: catalogNumber,
		})
	}

	for mediaIndex, media := range doc.Media {
		mediaPosition := media.Position
		if mediaPosition <= 0 {
			mediaPosition = mediaIndex + 1
		}
		trackCount := media.TrackCount
		if trackCount <= 0 {
			trackCount = len(media.Tracks)
		}
		m.ReleaseMedia = append(m.ReleaseMedia, model.ReleaseMediaRow{
			ReleaseMBID: mbid,
			Position:    mediaPosition,
			Format:      normalizeString(media.Format),
			TrackCount:  trackCount,
		})

		for trackIndex, track := range media.Tracks {
			trackMBID := normalizeString(track.ID)
			if trackMBID == "" || track.Recording == nil {
				continue
			}
			recordingMBID := normalizeString(track.Recording.ID)
			if recordingMBID == "" {
				continue
			}
			position := track.Position
			if position <= 0 {
				position = trackIndex + 1
			}
			number := normalizeString(track.Number)
			if number == "" {
				number = fmt.Sprintf("%d", position)
			}
			title := normalizeString(track.Title)
			if title == "" {
				title = normalizeString(track.Recording.Title)
			}

			m.Tracks = append(m.Tracks, model.TrackRow{
				MBID:          trackMBID,
				ReleaseMBID:   mbid,
				RecordingMBID: recordingMBID,
				MediaPosition: mediaPosition,
				Position:      position,
				Number:        number,
				Title:         title,
				Length:        intPtr(track.Length),
			})
			appendRecording(&m, track.Recording, false)
		}
	}

	return m, nil
}
