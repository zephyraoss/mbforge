package parser

import (
	"strings"

	"github.com/zephyraoss/mbforge/internal/model"
)

type tag struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type genre struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type relation struct {
	Type       string  `json:"type"`
	TargetType string  `json:"target-type"`
	URL        *urlRef `json:"url"`
}

type urlRef struct {
	Resource string `json:"resource"`
}

type areaRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type artistRef struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	SortName string `json:"sort-name"`
}

type labelRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type alias struct {
	Name      string `json:"name"`
	SortName  string `json:"sort-name"`
	Type      string `json:"type"`
	Locale    string `json:"locale"`
	IsPrimary bool   `json:"primary"`
}

type artistCredit struct {
	Name       string     `json:"name"`
	JoinPhrase string     `json:"joinphrase"`
	Artist     *artistRef `json:"artist"`
}

type textRepresentation struct {
	Language string `json:"language"`
	Script   string `json:"script"`
}

func normalizeString(v string) string {
	return strings.TrimSpace(v)
}

func normalizeTagRows(entityMBID string, tags []tag) []model.RecordingTagRow {
	seen := make(map[string]struct{}, len(tags))
	rows := make([]model.RecordingTagRow, 0, len(tags))
	for _, item := range tags {
		name := normalizeString(item.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		rows = append(rows, model.RecordingTagRow{
			RecordingMBID: entityMBID,
			Tag:           name,
			Count:         item.Count,
		})
	}
	return rows
}

func normalizeArtistCreditForReleaseGroup(entityMBID string, credits []artistCredit) []model.ReleaseGroupArtistRow {
	rows := make([]model.ReleaseGroupArtistRow, 0, len(credits))
	for i, credit := range credits {
		if credit.Artist == nil || normalizeString(credit.Artist.ID) == "" {
			continue
		}
		name := normalizeString(credit.Name)
		if name == "" {
			name = normalizeString(credit.Artist.Name)
		}
		rows = append(rows, model.ReleaseGroupArtistRow{
			ReleaseGroupMBID: entityMBID,
			ArtistMBID:       normalizeString(credit.Artist.ID),
			ArtistName:       name,
			JoinPhrase:       normalizeString(credit.JoinPhrase),
			Position:         i + 1,
		})
	}
	return rows
}

func normalizeArtistCreditForRelease(entityMBID string, credits []artistCredit) []model.ReleaseArtistRow {
	rows := make([]model.ReleaseArtistRow, 0, len(credits))
	for i, credit := range credits {
		if credit.Artist == nil || normalizeString(credit.Artist.ID) == "" {
			continue
		}
		name := normalizeString(credit.Name)
		if name == "" {
			name = normalizeString(credit.Artist.Name)
		}
		rows = append(rows, model.ReleaseArtistRow{
			ReleaseMBID: entityMBID,
			ArtistMBID:  normalizeString(credit.Artist.ID),
			ArtistName:  name,
			JoinPhrase:  normalizeString(credit.JoinPhrase),
			Position:    i + 1,
		})
	}
	return rows
}

func normalizeArtistCreditForRecording(entityMBID string, credits []artistCredit) []model.RecordingArtistRow {
	rows := make([]model.RecordingArtistRow, 0, len(credits))
	for i, credit := range credits {
		if credit.Artist == nil || normalizeString(credit.Artist.ID) == "" {
			continue
		}
		name := normalizeString(credit.Name)
		if name == "" {
			name = normalizeString(credit.Artist.Name)
		}
		rows = append(rows, model.RecordingArtistRow{
			RecordingMBID: entityMBID,
			ArtistMBID:    normalizeString(credit.Artist.ID),
			ArtistName:    name,
			JoinPhrase:    normalizeString(credit.JoinPhrase),
			Position:      i + 1,
		})
	}
	return rows
}

func normalizeExternalLinks(entityType, entityMBID string, relations []relation) []model.ExternalLinkRow {
	seen := make(map[string]struct{}, len(relations))
	rows := make([]model.ExternalLinkRow, 0, len(relations))
	for _, rel := range relations {
		if normalizeString(rel.TargetType) != "url" || rel.URL == nil {
			continue
		}
		relType := normalizeString(rel.Type)
		resource := normalizeString(rel.URL.Resource)
		if relType == "" || resource == "" {
			continue
		}
		key := relType + "\x00" + resource
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		rows = append(rows, model.ExternalLinkRow{
			EntityType: entityType,
			EntityMBID: entityMBID,
			RelType:    relType,
			URL:        resource,
		})
	}
	return rows
}

func intPtr(v *int) *int {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}
