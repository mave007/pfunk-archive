# Stage 5: Validate

Run quality checks, schema validation, and duplicate detection on the dataset.

## Scripts

- detect_song_name_fuzzy_duplicates.py -- find near-duplicate song names
- quality_gates.py -- check coverage, completeness, and FK integrity
- validate_discography.py -- field-level validation rules
- validate_schema.py -- verify CSV columns match expected schema

## Input

- data/discography.csv
- data/catalog_*.csv

## Output

- reports/duplicates/ (JSON and Markdown)
- reports/quality/ (JSON and Markdown)
- reports/validation/ (Markdown)

## Expected Result

All quality gates pass. Schema matches the 22-column spec. No high-confidence
duplicates remain unresolved.
