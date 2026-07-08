package parser

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/zephyraoss/mbforge/internal/model"
)

type workDoc struct {
	ID             string     `json:"id"`
	Title          string     `json:"title"`
	Disambiguation string     `json:"disambiguation"`
	Type           string     `json:"type"`
	Language       string     `json:"language"`
	Languages      []string   `json:"languages"`
	ISWCs          []string   `json:"iswcs"`
	Aliases        []alias    `json:"aliases"`
	Tags           []tag      `json:"tags"`
	Relations      []relation `json:"relations"`
}

func ParseWork(line []byte) (model.Mutation, error) {
	var doc workDoc
	if err := json.Unmarshal(line, &doc); err != nil {
		return model.Mutation{}, err
	}
	mbid := normalizeString(doc.ID)
	title := normalizeString(doc.Title)
	if mbid == "" || title == "" {
		return model.Mutation{}, fmt.Errorf("work missing id or title")
	}

	m := model.Mutation{
		Works: []model.WorkRow{{
			MBID:           mbid,
			Title:          title,
			Disambiguation: normalizeString(doc.Disambiguation),
			Type:           normalizeString(doc.Type),
			Languages:      normalizeLanguages(doc.Languages, doc.Language),
		}},
		ExternalLinks: normalizeExternalLinks("work", mbid, doc.Relations),
	}

	iswcSeen := make(map[string]struct{}, len(doc.ISWCs))
	for _, iswc := range doc.ISWCs {
		iswc = normalizeString(iswc)
		if iswc == "" {
			continue
		}
		if _, ok := iswcSeen[iswc]; ok {
			continue
		}
		iswcSeen[iswc] = struct{}{}
		m.WorkISWCs = append(m.WorkISWCs, model.WorkISWCRow{WorkMBID: mbid, ISWC: iswc})
	}

	aliasSeen := make(map[string]struct{}, len(doc.Aliases))
	for _, item := range doc.Aliases {
		name := normalizeString(item.Name)
		if name == "" {
			continue
		}
		locale := normalizeString(item.Locale)
		key := name + "\x00" + locale
		if _, ok := aliasSeen[key]; ok {
			continue
		}
		aliasSeen[key] = struct{}{}
		m.WorkAliases = append(m.WorkAliases, model.WorkAliasRow{
			WorkMBID:  mbid,
			Name:      name,
			SortName:  normalizeString(item.SortName),
			Type:      normalizeString(item.Type),
			Locale:    locale,
			IsPrimary: item.IsPrimary,
		})
	}

	tagSeen := make(map[string]struct{}, len(doc.Tags))
	for _, item := range doc.Tags {
		tagName := normalizeString(item.Name)
		if tagName == "" {
			continue
		}
		if _, ok := tagSeen[tagName]; ok {
			continue
		}
		tagSeen[tagName] = struct{}{}
		m.WorkTags = append(m.WorkTags, model.WorkTagRow{
			WorkMBID: mbid,
			Tag:      tagName,
			Count:    item.Count,
		})
	}

	linkSeen := make(map[string]struct{})
	for _, rel := range doc.Relations {
		if normalizeString(rel.TargetType) != "recording" || rel.Recording == nil {
			continue
		}
		recordingMBID := normalizeString(rel.Recording.ID)
		relType := normalizeString(rel.Type)
		if recordingMBID == "" || relType == "" {
			continue
		}
		attributes := normalizeRelationAttributes(rel.Attributes)
		key := strings.Join([]string{recordingMBID, relType, attributes}, "\x00")
		if _, ok := linkSeen[key]; ok {
			continue
		}
		linkSeen[key] = struct{}{}
		m.RecordingWorks = append(m.RecordingWorks, model.RecordingWorkRow{
			RecordingMBID: recordingMBID,
			WorkMBID:      mbid,
			Type:          relType,
			Attributes:    attributes,
		})
	}

	return m, nil
}
