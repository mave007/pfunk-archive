# Stage 1: Normalize

Deduplicate, strip prefixes, and standardize fields in discography.csv.

## Scripts

- canonicalize_discography.py -- normalize names, fix types, assign
  work/version IDs

## Input

- data/discography.csv

## Output

- data/discography.csv (updated in place)
- reports/canonicalization/ (JSON and Markdown reports)

## Expected Result

All rows have consistent casing, no song_name prefixes, valid
release_category/edition_type/version_type values, and stable work_id/version_id.
