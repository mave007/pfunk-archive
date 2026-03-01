# P-Funk Data Reliability Spec

## End Goal

Maintain a trustworthy, continuously updatable P-Funk canonical dataset
that is:

- correct,
- consistent,
- traceable,
- idempotent.

Website implementation is deferred until data quality is stable.

## Canonical Ownership

- Agent instructions and project rules: `AGENTS.md`
- Schema definitions: `scripts/schema.py`
- Data transforms and checks: `scripts/<N.stage>/`

Do not duplicate rule logic across files. Reference canonical owners.

## Source Schema (`data/discography.csv`)

### 22-Column Schema

```text
GROUP: Identity
 1  artist              string      Performing act name
 2  song_name           string      Song or release title (no prefixes)
 3  album_name          string      Release/album title
 4  track_position      string      N/M for tracks; empty for album/single rows

GROUP: Classification
 5  row_type            enum        album | track | single
 6  release_category    enum        original | reissue | compilation |
                                live | remix_album | soundtrack
 7  edition_type        enum        standard | expanded | deluxe | remaster |
                                remix | demo | bonus_track | alternate_mix
 8  version_type        enum        same_master | remix_or_edit |
                                live_recording | re_recording | unknown

GROUP: Release info
 9  release_date        string      YYYY, YYYY-MM, or YYYY-MM-DD
10  label               string      Record label and catalog number
11  era                 enum        6 eras from Pre-P-Funk through Legacy Era
12  genre               string      Comma-separated genre tags

GROUP: Recognition
13  chart_position       string      Billboard/chart placements
14  awards              string      Grammy/certification data

GROUP: Links
15  spotify_url         url         Spotify track or album URL
16  youtube_url         url         YouTube video or playlist URL
17  duration_seconds    float       Track duration from Spotify API

GROUP: Lineage
18  alternative_names   string      Alternate act names
19  source_release_id   string      Earliest non-compilation release with same work_id
20  work_id             string      Stable composition identity hash
21  version_id          string      Stable recording/mix identity hash
22  notes               string      Free-text annotations, provenance,
                                edge-case flags
```

## Field Discipline

- `track_position`: empty for non-tracks; otherwise `N/M`
- `release_date`: `YYYY`, `YYYY-MM`, or `YYYY-MM-DD`
- `spotify_url`: empty or `https://open.spotify.com/...`
- `youtube_url`: empty or `https://www.youtube.com/...` or
  `https://youtu.be/...`

## Derived Relational Outputs

Generate and maintain:

- `data/catalog_artists.csv`
- `data/catalog_releases.csv`
- `data/catalog_tracks.csv`
- `data/catalog_works.csv`
- `data/catalog_personnel.csv`

These files provide stable IDs and relationships for downstream use.

## Pipeline

Six stages, run in order. Each stage must complete before the next begins
unless noted otherwise.

```text
Stage 0: Discover         scripts/0.discover/
Stage 1: Normalize        scripts/1.normalize/
Stage 2: Build Catalog    scripts/2.catalog/
Stage 3: Reconcile        scripts/3.reconcile/
Stage 4: Enrich           scripts/4.enrich/
Stage 5: Validate         scripts/5.validate/
On-demand: Audit          scripts/audit/
```

See each stage's `README.md` for the full script list.

### Stage 0 Notes

- All scrapers and API crawlers can run in parallel.
- Each outputs to `data/discovery_raw/<source>.csv` in a common format.
- `consolidate_candidates.py` merges all sources, deduplicates, and scores.
- `merge_candidates.py --write` auto-merges high-confidence candidates.
- Deep exploration via `spider_site.py` is on-demand (monthly).
- All scripts support `--force` for cache bypass.
- Discovery seeds: `data/discovery_seeds.csv` (Discogs artist IDs).
- Confidence scoring: high (2+ sources or API match), medium (1 structured),
  low (1 unstructured).

### Stage 4 Notes

- Spotify and YouTube enrichment both write to `discography.csv` and must
  run sequentially.
- Discogs personnel enrichment writes to `data/catalog_personnel.csv` and
  can run in parallel with Spotify/YouTube.
- Backfill scripts use local cache only (no API calls) and run after
  the enrichment scripts.

### Stage 5 Notes

- `quality_gates.py` depends on output from
  `detect_song_name_fuzzy_duplicates.py` (run it first if reports are
  missing).
- All three validators can run in parallel since they are read-only.

### Spotify Link Quality Workflow

Run after Stage 5 when auditing existing Spotify URLs:

1. `scripts/audit/score_spotify_link_mismatches.py` -- score
2. `scripts/audit/quarantine_suspicious_spotify_links.py` -- clear
3. `scripts/audit/repopulate_spotify_high_confidence.py` -- re-search

### Queue Generation (On-Demand)

- `scripts/audit/generate_spotify_mismatch_queue.py` -- manual review
- `scripts/audit/generate_youtube_gap_queue.py` -- YouTube priority
- `scripts/5.validate/detect_song_name_fuzzy_duplicates.py` -- fuzzy
  duplicate detection

## Full Pipeline Run Order

Concrete steps for a complete pipeline execution:

```bash
python3 scripts/run_pipeline.py
```

See `README.md` for full manual stage-by-stage commands.

## Validation Checks

Must verify:

- schema validity,
- field format validity,
- dedupe integrity,
- tracking alignment,
- idempotency rerun proof.

## Compilation Variant Decision Matrix

Use this matrix when a compilation track may differ from the canonical
original.

### Identity Layers

- **Work**: composition identity (song concept)
- **Version**: recording/mix identity (studio take, remix, live, edit)
- **Release Track**: placement of a version on a specific release

### Deterministic Classification Rules

For each compilation track:

1. Map to a `work_id` using normalized base title.
2. Determine `version_type`:
   - `same_master`: no evidence of audio variation
   - `remix_or_edit`: title/source indicates remix, mix, edit, dub,
     instrumental, or extended version
   - `live_recording`: explicitly live performance or live source
   - `re_recording`: explicitly newly recorded/alternate performance
   - `unknown`: insufficient evidence
3. Resolve `source_release_id`:
   - choose earliest non-compilation release carrying same `work_id`
   - leave empty only when unresolved

### Compilation Status Derivation

Compilation status is derivable from
`row_type=track AND release_category=compilation`.

### Variant Signals

- title markers: `Remix`, `Mix`, `Edit`, `Dub`, `Extended`,
  `Instrumental`, `Live`, `Version`
- featured guest changes
- recording date differences
- duration delta compared to canonical source
- source metadata indicating alternate recording context

### Duration Rule

If duration differs by more than 3 seconds from the canonical source,
treat as potential version change and require explicit `version_type`.

## Duplicate and Original Resolution Contract

Use this contract for duplicate detection and original selection while
keeping canonical schema unchanged.

### Duplicate Relation Classes (Report-Level)

- `same_work_variant`: likely same underlying work with variant
  presentation across title/version context.
- `compilation_copy`: compilation track mapped to a non-compilation source
  release for the same work.
- `probable_cover_or_interpolation`: cross-artist similarity without
  sufficient same-work/version evidence.
- `uncertain`: similarity signals conflict or are insufficient.

### Variant Subtype Inference (From Existing Fields)

Infer subtype tags in reports only (do not add columns):

- `radio_edit`: inferred from title/notes markers or `edition_type` values
  indicating edit/radio version.
- `remix`: inferred from `version_type=remix_or_edit` and title/notes
  remix markers.
- `remastered`: inferred from `edition_type=remaster` or title markers.
- `live`: inferred from explicit title markers or `version_type=live_recording`.
- `re_recording`: inferred from `version_type=re_recording`.
- `compilation_copy`: inferred from
  `row_type=track AND release_category=compilation` with
  resolved `source_release_id`.

### Deterministic Original Selection

Within each high-confidence duplicate cluster, select one
`original_candidate` using this stable ranking:

1. prefer inferred studio context (non-live, non-re-recording)
2. prefer `release_category != compilation`
3. prefer `version_type=same_master`
4. prefer earliest valid `release_date`
5. tie-breaker: non-empty `track_position`
6. final tie-breaker: deterministic lexical key

### Cross-Artist Resolution

Cross-artist matches must be split into:

- `same_work_variant` when title/version/source signals are consistent, or
- `probable_cover_or_interpolation` when similarity is high but source
  equivalence is not supported.

Do not collapse cross-artist rows into one canonical row without manual
approval.

## Enrichment Pipelines

### Spotify

- cache-first via `scripts/4.enrich/enrich_spotify.py`
- backfill via `scripts/4.enrich/backfill_spotify_from_cache.py`
- duration backfill via `scripts/4.enrich/backfill_duration_from_cache.py`
- verification via `scripts/audit/score_spotify_link_mismatches.py`
- quarantine via `scripts/audit/quarantine_suspicious_spotify_links.py`
- repopulation via
  `scripts/audit/repopulate_spotify_high_confidence.py`

### YouTube

- cache-first via `scripts/4.enrich/enrich_youtube.py`
- YouTube Data API v3
- 100 searches/day free tier
- gap queue via `scripts/audit/generate_youtube_gap_queue.py`

### Discogs Personnel

- via `scripts/4.enrich/enrich_personnel_from_discogs.py`
- cache in `data/.discogs_cache/`

## Script Map

See each stage's `README.md` for the full script list.

- `scripts/0.discover/` -- Stage 0: Discovery
- `scripts/1.normalize/` -- Stage 1: Normalize
- `scripts/2.catalog/` -- Stage 2: Build Catalog
- `scripts/3.reconcile/` -- Stage 3: Reconcile
- `scripts/4.enrich/` -- Stage 4: Enrich
- `scripts/5.validate/` -- Stage 5: Validate
- `scripts/audit/` -- On-demand: Audit

## Final Rule

When forced to choose between speed and correctness, choose correctness
and attach explicit evidence to every pipeline run decision.
