# Stage 2: Build Catalog

Generate relational views from the flat discography.csv.

## Scripts

- build_catalog_relations.py -- produce normalized artist, release,
  track, and work tables

## Input

- data/discography.csv

## Output

- data/catalog_artists.csv
- data/catalog_releases.csv
- data/catalog_tracks.csv
- data/catalog_works.csv

## Expected Result

Four CSV files with stable IDs (art_, rel_, trk_, wrk_ prefixes) that
provide relational lookups across the discography.
