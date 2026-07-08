# mbforge

`mbforge` is a Go CLI that downloads the latest MusicBrainz JSON dump and builds a flattened libSQL metadata database for Oxygen/Chromakopia. `mbforge sync` keeps that database current by applying the hourly incremental JSON dumps from the MusicBrainz Live Data Feed.

It is intentionally narrow:

- Full builds plus hourly incremental sync (no deletions/merges, see below)
- No raw JSON blobs
- No annotation import

## Commands

`mbforge build`

- Resolves the latest dump directory from `LATEST`
- Downloads the selected `*.tar.xz` archives into `--dump-dir`
- Streams `xz -> tar -> JSONL` without extracting the full dumps to disk
- Imports artists (incl. artist–artist relationships), labels, works (incl. recording→work links), release groups, releases, embedded recordings/tracks, and standalone recordings
- Defers secondary index creation until after the bulk load
- Optionally builds a full-text search index with `--search-index`
- Writes `_meta` with dump and replication metadata
- Runs `PRAGMA optimize` and `VACUUM`

`mbforge sync`

- Applies MusicBrainz Live Data Feed incremental JSON dumps to a database built by `mbforge build`
- Requires a [MetaBrainz Live Data Feed access token](https://metabrainz.org/supporters/account-type) (`--token` or `$MBFORGE_LDF_TOKEN`)
- Resumes from `_meta.replication_sequence` and fetches every packet up to the current one reported by the `replication-info` endpoint
- Applies one packet per transaction and advances `_meta.replication_sequence` in the same transaction, so an interrupted sync can simply be rerun
- A 404 for an entity archive means the packet has no changes for that entity and is skipped
- Runs in WAL mode with `synchronous=NORMAL`, so the database stays safe for concurrent readers (the aggressive build-time pragmas are never used); note the database file remains in WAL mode afterwards
- Updates the full-text search index rows for changed entities when the search index exists
- Creates any tables added by newer mbforge versions (labels, works, relationship link tables) on databases built before them, so older databases keep syncing; their historical label/work rows still require a full rebuild to backfill
- Stops with an error instructing a full rebuild if a packet's `SCHEMA_SEQUENCE` differs from `_meta.schema_sequence`

Known limitation: the incremental JSON dumps do not carry deletions or merges, so removed or merged entities linger until the next full rebuild. The operational answer is a periodic `mbforge build` (for example monthly) layered under hourly `mbforge sync` runs.

`mbforge info`

- Prints database path, size, core row counts, and `_meta` values

`mbforge search`

- Searches artists, labels, works, release groups, releases, recordings, and tracks from a single free-form query
- Accepts artist names, track titles, album titles, MBIDs, ISRCs, ISWCs, and release barcodes
- Labels and works are only covered by the fast indexed path, not the slow SQL fallback
- Uses the full-text search index when present
- Falls back to the older slower SQL path when the search index is absent

`mbforge search-index`

- Builds or rebuilds the full-text search index on an existing database
- Useful when you already finished a long `build` run without `--search-index`

`mbforge version`

- Prints build-time version metadata

## Requirements

- Go 1.24+
- CGO-enabled build environment
- Network access to the MusicBrainz dump mirror

## Build

```bash
go build ./cmd/mbforge
```

Example:

```bash
mbforge build \
  --output /mnt/nvme/metadata.db \
  --dump-dir /mnt/nvme/mbdump \
  --workers 16 \
  --batch-size 5000 \
  --search-index \
  --verbose
```

Import a subset of entities:

```bash
mbforge build \
  --output ./musicbrainz.db \
  --entities artist,release-group
```

Apply the latest Live Data Feed packets to an existing database:

```bash
export MBFORGE_LDF_TOKEN=your-metabrainz-token
mbforge sync \
  --db /mnt/nvme/metadata.db \
  --dump-dir /mnt/nvme/mbdump
```

Inspect a finished database:

```bash
mbforge info --db ./musicbrainz.db
```

Search across the main entities:

```bash
mbforge search --db ./musicbrainz.db "nirvana"
```

Build the fast search index on an existing database:

```bash
mbforge search-index --db ./musicbrainz.db
```

## Pipeline

Import order:

1. `artist`
2. `label`
3. `work`
4. `release-group`
5. `release`
6. `recording`

Important detail: `release.tar.xz` contains the overwhelming majority of recordings at `media[].tracks[].recording`. `recording.tar.xz` only covers the standalone subset. `mbforge` imports both, and uses `INSERT OR IGNORE` for `recordings` so the two sources can coexist safely.

The same pattern applies to works: `work.tar.xz` is imported first so its full rows win, and works embedded in recording work-relations (in both `recording.tar.xz` and `release.tar.xz`) only fill gaps via `INSERT OR IGNORE`.

The importer uses multiple JSON parse workers and a single batched SQLite writer to avoid write contention.

## Schema Notes

- Areas are flattened inline instead of being normalized into separate tables.
- Labels are first-class rows in `labels` (with `label_aliases`, `label_tags`, `label_genres`); `release_labels.label_mbid` joins releases to them while keeping the flattened `label_name`/`catalog_number` columns.
- Works live in `works` (with `work_aliases`, `work_iswcs`, `work_tags`); `recording_works` links recordings to works from the `performance` relationship, so covers/live versions of the same song connect through the shared work. `attributes` holds a sorted JSON array like `["cover","live"]` (empty string when the relation has no attributes).
- `recording_works` rows are extracted from both sides of the relationship (recording lines and work lines) and deduplicate via `INSERT OR IGNORE`; rows from a work line may reference recordings imported later in the pipeline, which is why `recording_works.recording_mbid` carries no foreign-key clause.
- `artist_relationships` stores artist–artist relationships (band membership, collaborations, teachers, …) with one row per direction as rendered in each artist's dump line: query it by `artist_mbid` to get everything that artist participates in. `begin_date`/`end_date`/`attributes` are part of the primary key because the same pair can hold the same relationship type over several stints (empty strings stand in for NULLs there, as in `release_labels`).
- Secondary indexes are created after bulk import for speed.
- SQLite does not allow expressions inside a table primary key, so nullable key parts from the draft schema are normalized to empty strings for `artist_aliases.locale` and `release_labels` key columns.

## Azure Build VM

[`deploy/cloud-init.yaml`](./deploy/cloud-init.yaml) is a starting point for the one-shot build VM path:

1. Install Go
2. Clone the repo
3. Build `mbforge`
4. Build `metadata.db` on local NVMe
5. Copy the finished database to an attached disk
6. Stop the VM

You still need to customize the final handoff step to your long-running `sqld` VM.
