# Stage 4: Enrich

Populate missing metadata in discography.csv using external APIs and local caches.

## Scripts

- enrich_spotify.py -- search Spotify for missing URLs and metadata
- enrich_youtube.py -- search YouTube for missing video URLs
- enrich_personnel_from_discogs.py -- fetch credits from Discogs
- backfill_duration_from_cache.py -- fill duration_seconds from Spotify cache
- backfill_spotify_from_cache.py -- fill empty spotify_url from cache

## Input

- data/discography.csv
- data/url_search_log.json
- data/catalog_releases.csv (Discogs enrichment)
- API caches in data/.spotify_cache/, data/.youtube_cache/, data/.discogs_cache/

## Output

- data/discography.csv (updated in place, additive only)
- data/catalog_personnel.csv (Discogs credits)
- API caches populated with new entries

## Expected Result

Rows with empty spotify_url, youtube_url, or duration_seconds are filled
where API matches exist. Personnel credits are written to catalog_personnel.csv.
No existing data is overwritten or deleted.
