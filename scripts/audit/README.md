# On-Demand: Audit

Score, quarantine, and repopulate Spotify/YouTube URLs when quality is suspect.

## Scripts

- score_spotify_link_mismatches.py -- compare cached metadata to discography fields
- quarantine_suspicious_spotify_links.py -- clear URLs that fail confidence checks
- repopulate_spotify_high_confidence.py -- re-fill from cache with verified matches
- generate_spotify_mismatch_queue.py -- export rows needing manual Spotify review
- generate_youtube_gap_queue.py -- export rows missing YouTube URLs

## Input

- data/discography.csv
- data/url_search_log.json
- data/.spotify_cache/

## Output

- data/discography.csv (updated when --write is passed)
- reports/spotify_audit/ (JSON, Markdown, and CSV queues)

## Expected Result

Suspicious URLs are quarantined, high-confidence matches repopulated, and
remaining mismatches queued for manual review.
