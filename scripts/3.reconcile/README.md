# Stage 3: Reconcile

Cross-check discography.csv against url_search_log.json for consistency.

## Scripts

- reconcile_tracking.py -- verify URL search entries match discography rows

## Input

- data/discography.csv
- data/url_search_log.json

## Output

- data/url_search_log.json (updated when --write is passed)
- reports/reconciliation/ (JSON report)

## Expected Result

Every discography row with a spotify_url or youtube_url has a matching
entry in url_search_log.json. Orphaned log entries are flagged.
