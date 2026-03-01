# Stage 0: Discover

Find new releases, tracks, and metadata from web sources and APIs that
may be missing from discography.csv.

## Regular Discovery

Run these to check known sources for new data:

- scrape_wikipedia_pfunk.py -- parse Wikipedia's List of P-Funk projects
- scrape_motherpage.py -- parse The Motherpage album list
- scrape_georgeclinton.py -- parse georgeclinton.com discography
- scrape_pfunk_forums.py -- parse P-Funk Forums topics via Discourse API
- discover_from_discogs.py -- crawl Discogs artist releases from seed list
- discover_from_spotify.py -- crawl Spotify artist albums from catalog

## Deep Exploration

Run occasionally (monthly or on-demand) for thorough site-wide crawling:

- spider_site.py -- generic site spider with per-domain extraction profiles

## Consolidation and Merge

Run after any discovery scripts to process results:

- consolidate_candidates.py -- merge all discovery_raw CSVs, deduplicate, score
- merge_candidates.py -- auto-merge high-confidence candidates into discography.csv

## Input

- data/discovery_seeds.csv (Discogs crawlers)
- data/catalog_artists.csv (Spotify crawler)
- data/discography.csv (consolidation diffing)

## Output

- data/discovery_raw/*.csv (per-source intermediate files, gitignored)
- data/discovery_candidates.csv (consolidated candidates, gitignored)
- data/discovery_log.json (run metadata)
- reports/discovery/review_queue.csv (candidates needing manual review)
- reports/discovery/corrections_queue.csv (metadata discrepancies)

## Expected Result

New releases are discovered, scored for confidence, and either auto-merged
into discography.csv (high confidence) or queued for manual review. No
existing data is overwritten or deleted.
