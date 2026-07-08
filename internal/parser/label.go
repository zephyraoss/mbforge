package parser

import (
	"encoding/json"
	"fmt"

	"github.com/zephyraoss/mbforge/internal/model"
)

type labelDoc struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	SortName       string     `json:"sort-name"`
	Disambiguation string     `json:"disambiguation"`
	Type           string     `json:"type"`
	LabelCode      *int       `json:"label-code"`
	Country        string     `json:"country"`
	Area           *areaRef   `json:"area"`
	LifeSpan       lifeSpan   `json:"life-span"`
	Aliases        []alias    `json:"aliases"`
	Tags           []tag      `json:"tags"`
	Genres         []genre    `json:"genres"`
	Relations      []relation `json:"relations"`
}

func ParseLabel(line []byte) (model.Mutation, error) {
	var doc labelDoc
	if err := json.Unmarshal(line, &doc); err != nil {
		return model.Mutation{}, err
	}
	mbid := normalizeString(doc.ID)
	if mbid == "" {
		return model.Mutation{}, fmt.Errorf("label missing id")
	}
	name := normalizeString(doc.Name)
	if name == "" {
		return model.Mutation{}, fmt.Errorf("label %s missing name", mbid)
	}
	sortName := normalizeString(doc.SortName)
	if sortName == "" {
		sortName = name
	}

	m := model.Mutation{
		Labels: []model.LabelRow{{
			MBID:           mbid,
			Name:           name,
			SortName:       sortName,
			Disambiguation: normalizeString(doc.Disambiguation),
			Type:           normalizeString(doc.Type),
			LabelCode:      intPtr(doc.LabelCode),
			Country:        normalizeString(doc.Country),
			BeginDate:      normalizeString(doc.LifeSpan.Begin),
			EndDate:        normalizeString(doc.LifeSpan.End),
			Ended:          doc.LifeSpan.Ended,
		}},
		ExternalLinks: normalizeExternalLinks("label", mbid, doc.Relations),
	}

	if doc.Area != nil {
		m.Labels[0].AreaMBID = normalizeString(doc.Area.ID)
		m.Labels[0].AreaName = normalizeString(doc.Area.Name)
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
		m.LabelAliases = append(m.LabelAliases, model.LabelAliasRow{
			LabelMBID: mbid,
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
		m.LabelTags = append(m.LabelTags, model.LabelTagRow{
			LabelMBID: mbid,
			Tag:       tagName,
			Count:     item.Count,
		})
	}

	genreSeen := make(map[string]struct{}, len(doc.Genres))
	for _, item := range doc.Genres {
		genreID := normalizeString(item.ID)
		genreName := normalizeString(item.Name)
		if genreID == "" || genreName == "" {
			continue
		}
		if _, ok := genreSeen[genreID]; ok {
			continue
		}
		genreSeen[genreID] = struct{}{}
		m.LabelGenres = append(m.LabelGenres, model.LabelGenreRow{
			LabelMBID: mbid,
			GenreMBID: genreID,
			GenreName: genreName,
			Count:     item.Count,
		})
	}

	return m, nil
}
