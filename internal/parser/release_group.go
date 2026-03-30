package parser

import (
	"encoding/json"
	"fmt"

	"github.com/zephyraoss/mbforge/internal/model"
)

type releaseGroupDoc struct {
	ID               string         `json:"id"`
	Title            string         `json:"title"`
	PrimaryType      string         `json:"primary-type"`
	SecondaryTypes   []string       `json:"secondary-types"`
	Disambiguation   string         `json:"disambiguation"`
	FirstReleaseDate string         `json:"first-release-date"`
	ArtistCredit     []artistCredit `json:"artist-credit"`
	Tags             []tag          `json:"tags"`
	Relations        []relation     `json:"relations"`
}

func ParseReleaseGroup(line []byte) (model.Mutation, error) {
	var doc releaseGroupDoc
	if err := json.Unmarshal(line, &doc); err != nil {
		return model.Mutation{}, err
	}
	mbid := normalizeString(doc.ID)
	title := normalizeString(doc.Title)
	if mbid == "" || title == "" {
		return model.Mutation{}, fmt.Errorf("release-group missing id or title")
	}

	m := model.Mutation{
		ReleaseGroups: []model.ReleaseGroupRow{{
			MBID:             mbid,
			Title:            title,
			PrimaryType:      normalizeString(doc.PrimaryType),
			Disambiguation:   normalizeString(doc.Disambiguation),
			FirstReleaseDate: normalizeString(doc.FirstReleaseDate),
		}},
		ReleaseGroupArtists: normalizeArtistCreditForReleaseGroup(mbid, doc.ArtistCredit),
		ExternalLinks:       normalizeExternalLinks("release_group", mbid, doc.Relations),
	}

	typeSeen := make(map[string]struct{}, len(doc.SecondaryTypes))
	for _, secondaryType := range doc.SecondaryTypes {
		secondaryType = normalizeString(secondaryType)
		if secondaryType == "" {
			continue
		}
		if _, ok := typeSeen[secondaryType]; ok {
			continue
		}
		typeSeen[secondaryType] = struct{}{}
		m.ReleaseGroupSecondaryTypes = append(m.ReleaseGroupSecondaryTypes, model.ReleaseGroupSecondaryTypeRow{
			ReleaseGroupMBID: mbid,
			Type:             secondaryType,
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
		m.ReleaseGroupTags = append(m.ReleaseGroupTags, model.ReleaseGroupTagRow{
			ReleaseGroupMBID: mbid,
			Tag:              tagName,
			Count:            item.Count,
		})
	}

	return m, nil
}
