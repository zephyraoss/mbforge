# mbforge

`mbforge` is a Go CLI that downloads the latest MusicBrainz JSON dump and builds a flattened libSQL metadata database for Oxygen/Chromakopia.

It is intentionally narrow:

- One-shot initial build only
- No incremental replication sync
- No raw JSON blobs
- No annotation import

Incremental updates belong in Chromakopia, which resumes from `_meta.replication_sequence`.

## Commands

`mbforge build`

- Resolves the latest dump directory from `LATEST`
- Downloads the selected `*.tar.xz` archives into `--dump-dir`
- Streams `xz -> tar -> JSONL` without extracting the full dumps to disk
- Imports artists, release groups, releases, embedded recordings/tracks, and standalone recordings
- Defers secondary index creation until after the bulk load
- Optionally builds a full-text search index with `--search-index`
- Writes `_meta` with dump and replication metadata
- Runs `PRAGMA optimize` and `VACUUM`

`mbforge info`

- Prints database path, size, core row counts, and `_meta` values

`mbforge search`

- Searches artists, release groups, releases, recordings, and tracks from a single free-form query
- Accepts artist names, track titles, album titles, MBIDs, ISRCs, and release barcodes
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
2. `release-group`
3. `release`
4. `recording`

Important detail: `release.tar.xz` contains the overwhelming majority of recordings at `media[].tracks[].recording`. `recording.tar.xz` only covers the standalone subset. `mbforge` imports both, and uses `INSERT OR IGNORE` for `recordings` so the two sources can coexist safely.

The importer uses multiple JSON parse workers and a single batched SQLite writer to avoid write contention.

## Schema Notes

- Areas and labels are flattened inline instead of being normalized into separate tables.
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
