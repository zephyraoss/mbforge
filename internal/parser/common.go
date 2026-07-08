package parser

import (
	"encoding/json"
	"sort"
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
	Type       string        `json:"type"`
	TargetType string        `json:"target-type"`
	Direction  string        `json:"direction"`
	Begin      string        `json:"begin"`
	End        string        `json:"end"`
	Ended      bool          `json:"ended"`
	Attributes []string      `json:"attributes"`
	URL        *urlRef       `json:"url"`
	Artist     *artistRef    `json:"artist"`
	Work       *workRef      `json:"work"`
	Recording  *recordingRef `json:"recording"`
}

type workRef struct {
	ID        string   `json:"id"`
	Title     string   `json:"title"`
	Type      string   `json:"type"`
	Language  string   `json:"language"`
	Languages []string `json:"languages"`
	ISWCs     []string `json:"iswcs"`
}

type recordingRef struct {
	ID string `json:"id"`
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

func normalizeRelationAttributes(attributes []string) string {
	cleaned := make([]string, 0, len(attributes))
	seen := make(map[string]struct{}, len(attributes))
	for _, attr := range attributes {
		attr = normalizeString(attr)
		if attr == "" {
			continue
		}
		if _, ok := seen[attr]; ok {
			continue
		}
		seen[attr] = struct{}{}
		cleaned = append(cleaned, attr)
	}
	if len(cleaned) == 0 {
		return ""
	}
	sort.Strings(cleaned)
	encoded, err := json.Marshal(cleaned)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func normalizeLanguages(languages []string, fallback string) string {
	cleaned := make([]string, 0, len(languages))
	seen := make(map[string]struct{}, len(languages))
	for _, lang := range languages {
		lang = normalizeString(lang)
		if lang == "" {
			continue
		}
		if _, ok := seen[lang]; ok {
			continue
		}
		seen[lang] = struct{}{}
		cleaned = append(cleaned, lang)
	}
	if len(cleaned) == 0 {
		return normalizeString(fallback)
	}
	return strings.Join(cleaned, ",")
}

func normalizeArtistRelationships(artistMBID string, relations []relation) []model.ArtistRelationshipRow {
	seen := make(map[string]struct{})
	var rows []model.ArtistRelationshipRow
	for _, rel := range relations {
		if normalizeString(rel.TargetType) != "artist" || rel.Artist == nil {
			continue
		}
		relatedMBID := normalizeString(rel.Artist.ID)
		relType := normalizeString(rel.Type)
		if relatedMBID == "" || relType == "" {
			continue
		}
		row := model.ArtistRelationshipRow{
			ArtistMBID:        artistMBID,
			RelatedArtistMBID: relatedMBID,
			RelatedArtistName: normalizeString(rel.Artist.Name),
			Type:              relType,
			Direction:         normalizeString(rel.Direction),
			BeginDate:         normalizeString(rel.Begin),
			EndDate:           normalizeString(rel.End),
			Ended:             rel.Ended,
			Attributes:        normalizeRelationAttributes(rel.Attributes),
		}
		key := strings.Join([]string{row.RelatedArtistMBID, row.Type, row.Direction, row.BeginDate, row.EndDate, row.Attributes}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		rows = append(rows, row)
	}
	return rows
}

func appendWorkRelations(m *model.Mutation, recordingMBID string, relations []relation) {
	linkSeen := make(map[string]struct{})
	workSeen := make(map[string]struct{})
	for _, rel := range relations {
		if normalizeString(rel.TargetType) != "work" || rel.Work == nil {
			continue
		}
		workMBID := normalizeString(rel.Work.ID)
		relType := normalizeString(rel.Type)
		if workMBID == "" || relType == "" {
			continue
		}
		attributes := normalizeRelationAttributes(rel.Attributes)
		key := strings.Join([]string{workMBID, relType, attributes}, "\x00")
		if _, ok := linkSeen[key]; !ok {
			linkSeen[key] = struct{}{}
			m.RecordingWorks = append(m.RecordingWorks, model.RecordingWorkRow{
				RecordingMBID: recordingMBID,
				WorkMBID:      workMBID,
				Type:          relType,
				Attributes:    attributes,
			})
		}

		title := normalizeString(rel.Work.Title)
		if title == "" {
			continue
		}
		if _, ok := workSeen[workMBID]; ok {
			continue
		}
		workSeen[workMBID] = struct{}{}
		m.Works = append(m.Works, model.WorkRow{
			MBID:      workMBID,
			Title:     title,
			Type:      normalizeString(rel.Work.Type),
			Languages: normalizeLanguages(rel.Work.Languages, rel.Work.Language),
		})
		iswcSeen := make(map[string]struct{}, len(rel.Work.ISWCs))
		for _, iswc := range rel.Work.ISWCs {
			iswc = normalizeString(iswc)
			if iswc == "" {
				continue
			}
			if _, ok := iswcSeen[iswc]; ok {
				continue
			}
			iswcSeen[iswc] = struct{}{}
			m.WorkISWCs = append(m.WorkISWCs, model.WorkISWCRow{WorkMBID: workMBID, ISWC: iswc})
		}
	}
}

func intPtr(v *int) *int {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}
