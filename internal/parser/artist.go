package parser

import (
	"encoding/json"
	"fmt"

	"github.com/zephyraoss/mbforge/internal/model"
)

type artistDoc struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	SortName       string     `json:"sort-name"`
	Disambiguation string     `json:"disambiguation"`
	Type           string     `json:"type"`
	Country        string     `json:"country"`
	Gender         string     `json:"gender"`
	Area           *areaRef   `json:"area"`
	LifeSpan       lifeSpan   `json:"life-span"`
	Aliases        []alias    `json:"aliases"`
	Tags           []tag      `json:"tags"`
	Genres         []genre    `json:"genres"`
	Relations      []relation `json:"relations"`
}

type lifeSpan struct {
	Begin string `json:"begin"`
	End   string `json:"end"`
	Ended bool   `json:"ended"`
}

func ParseArtist(line []byte) (model.Mutation, error) {
	var doc artistDoc
	if err := json.Unmarshal(line, &doc); err != nil {
		return model.Mutation{}, err
	}
	mbid := normalizeString(doc.ID)
	if mbid == "" {
		return model.Mutation{}, fmt.Errorf("artist missing id")
	}
	name := normalizeString(doc.Name)
	sortName := normalizeString(doc.SortName)
	if name == "" || sortName == "" {
		return model.Mutation{}, fmt.Errorf("artist %s missing required names", mbid)
	}

	m := model.Mutation{
		Artists: []model.ArtistRow{{
			MBID:           mbid,
			Name:           name,
			SortName:       sortName,
			Disambiguation: normalizeString(doc.Disambiguation),
			Type:           normalizeString(doc.Type),
			Country:        normalizeString(doc.Country),
			Gender:         normalizeString(doc.Gender),
			BeginDate:      normalizeString(doc.LifeSpan.Begin),
			EndDate:        normalizeString(doc.LifeSpan.End),
			Ended:          doc.LifeSpan.Ended,
		}},
		ExternalLinks: normalizeExternalLinks("artist", mbid, doc.Relations),
	}

	if doc.Area != nil {
		m.Artists[0].AreaMBID = normalizeString(doc.Area.ID)
		m.Artists[0].AreaName = normalizeString(doc.Area.Name)
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
		m.ArtistAliases = append(m.ArtistAliases, model.ArtistAliasRow{
			ArtistMBID: mbid,
			Name:       name,
			SortName:   normalizeString(item.SortName),
			Type:       normalizeString(item.Type),
			Locale:     locale,
			IsPrimary:  item.IsPrimary,
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
		m.ArtistTags = append(m.ArtistTags, model.ArtistTagRow{
			ArtistMBID: mbid,
			Tag:        tagName,
			Count:      item.Count,
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
		m.ArtistGenres = append(m.ArtistGenres, model.ArtistGenreRow{
			ArtistMBID: mbid,
			GenreMBID:  genreID,
			GenreName:  genreName,
			Count:      item.Count,
		})
	}

	return m, nil
}
