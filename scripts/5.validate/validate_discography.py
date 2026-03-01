#!/usr/bin/env python3
"""
Discography Validation

Comprehensive validation ensuring data quality, consistency, and
completeness across the canonical discography dataset.
"""

import csv
import json
import re
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import DISCOGRAPHY_COLUMNS as EXPECTED_ORDER  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent.parent
CSV_FILE = ROOT / 'data' / 'discography.csv'
TRACKING_FILE = ROOT / 'data' / 'url_search_log.json'
REPORT_DIR = ROOT / 'reports' / 'validation'
REPORT_FILE = REPORT_DIR / 'discography_latest.md'

class DiscographyValidator:
    def __init__(self):
        self.rows = []
        self.tracking = None
        self.issues = []
        self.stats = {}

    def load_data(self):
        """Load CSV and tracking file."""
        print("Loading data files...")

        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.rows = list(reader)
            self.fieldnames = reader.fieldnames

        if TRACKING_FILE.exists():
            with open(TRACKING_FILE, 'r', encoding='utf-8') as f:
                self.tracking = json.load(f)
            print(f"   Loaded {len(self.tracking['entries'])} tracking entries")
        else:
            self.tracking = {"entries": []}
            print("   Warning: tracking file not found -- skipping tracking checks")

        print(f"   Loaded {len(self.rows)} CSV rows")

    def validate_schema(self):
        """Validate CSV schema structure."""
        print("\nValidating schema structure...")

        expected_cols = len(EXPECTED_ORDER)

        if len(self.fieldnames) != expected_cols:
            self.issues.append(f"Schema: Expected {expected_cols} columns, found {len(self.fieldnames)}")

        if list(self.fieldnames) != EXPECTED_ORDER:
            self.issues.append(f"Schema: Column order doesn't match expected order")

        self.stats['schema_valid'] = len(self.fieldnames) == expected_cols

    def validate_data_types(self):
        """Validate field data types and formats."""
        print("Validating data types and formats...")

        issues = []

        for idx, row in enumerate(self.rows, 1):
            if row.get('row_type') not in ['album', 'track', 'single', '']:
                issues.append(f"Row {idx}: Invalid row_type '{row.get('row_type')}'")

            valid_cats = ['original', 'reissue', 'compilation', 'live', 'remix_album', 'soundtrack', '']
            if row.get('release_category') not in valid_cats:
                issues.append(f"Row {idx}: Invalid release_category '{row.get('release_category')}'")

            release_date = row.get('release_date', '')
            if release_date and not re.match(r'^\d{4}(-\d{2})?(-\d{2})?$', release_date):
                issues.append(f"Row {idx}: Invalid release_date format '{release_date}'")

            track_pos = row.get('track_position', '')
            if track_pos and not re.match(r'^\d+(/\d+)?$', track_pos):
                issues.append(f"Row {idx}: Invalid track_position format '{track_pos}'")

            spotify_url = row.get('spotify_url', '')
            if spotify_url and not spotify_url.startswith('https://open.spotify.com/'):
                issues.append(f"Row {idx}: Invalid spotify_url format")

        self.stats['data_type_issues'] = len(issues)
        self.issues.extend(issues[:10])

    def validate_row_type_alignment(self):
        """Validate row_type aligns with track_position."""
        print("Validating row_type/track_position alignment...")

        tracks_no_pos = [r for r in self.rows if r.get('row_type') == 'track' and not r.get('track_position')]
        albums_with_pos = [r for r in self.rows if r.get('row_type') == 'album' and r.get('track_position')]

        if tracks_no_pos:
            self.issues.append(f"Alignment: {len(tracks_no_pos)} tracks missing track_position")
        if albums_with_pos:
            self.issues.append(f"Alignment: {len(albums_with_pos)} albums have track_position")

        self.stats['alignment_issues'] = len(tracks_no_pos) + len(albums_with_pos)

    def validate_completeness(self):
        """Validate data completeness for required fields."""
        print("Validating data completeness...")

        required_always = ['artist', 'song_name', 'album_name', 'release_date', 'era', 'genre']
        required_for_tracks = ['track_position']

        completeness = {}

        for field in required_always:
            missing = len([r for r in self.rows if not r.get(field)])
            completeness[field] = {
                'missing': missing,
                'pct': (len(self.rows) - missing) / len(self.rows) * 100
            }

        tracks = [r for r in self.rows if r.get('row_type') == 'track']
        tracks_missing_pos = len([r for r in tracks if not r.get('track_position')])
        completeness['track_position'] = {
            'missing': tracks_missing_pos,
            'pct': (len(tracks) - tracks_missing_pos) / len(tracks) * 100 if tracks else 100
        }

        self.stats['completeness'] = completeness

        for field, data in completeness.items():
            if data['pct'] < 100:
                self.issues.append(f"Completeness: {field} - {data['missing']} missing ({data['pct']:.1f}% complete)")

    def validate_url_coverage(self):
        """Validate URL coverage and tracking alignment."""
        print("Validating URL coverage...")

        has_spotify = len([r for r in self.rows if r.get('spotify_url')])
        has_youtube = len([r for r in self.rows if r.get('youtube_url')])
        has_any = len([r for r in self.rows if r.get('spotify_url') or r.get('youtube_url')])

        era_coverage = {}
        for row in self.rows:
            era = row.get('era', 'Unknown')
            if era not in era_coverage:
                era_coverage[era] = {'total': 0, 'spotify': 0}
            era_coverage[era]['total'] += 1
            if row.get('spotify_url'):
                era_coverage[era]['spotify'] += 1

        self.stats['url_coverage'] = {
            'spotify': has_spotify,
            'youtube': has_youtube,
            'any_url': has_any,
            'spotify_pct': has_spotify / len(self.rows) * 100,
            'youtube_pct': has_youtube / len(self.rows) * 100,
            'any_url_pct': has_any / len(self.rows) * 100,
            'by_era': era_coverage
        }

    def validate_chart_data(self):
        """Validate chart data for major albums."""
        print("Validating chart data...")

        major_albums = [r for r in self.rows if
                       r.get('row_type') == 'album' and
                       r.get('release_category') == 'original' and
                       r.get('release_date', '')[:4].isdigit() and
                       1970 <= int(r.get('release_date', '')[:4]) <= 1985]

        with_chart = [r for r in major_albums if r.get('chart_position')]
        with_awards = [r for r in major_albums if r.get('awards')]

        self.stats['chart_data'] = {
            'major_albums_1970_1985': len(major_albums),
            'with_chart_position': len(with_chart),
            'with_awards': len(with_awards),
            'chart_pct': len(with_chart) / len(major_albums) * 100 if major_albums else 0
        }

    def validate_tracking_alignment(self):
        """Validate tracking file aligns with CSV."""
        print("Validating tracking file alignment...")

        entries = self.tracking.get('entries', [])
        if not entries:
            self.stats['tracking_aligned'] = False
            return

        if len(self.rows) != len(entries):
            self.issues.append(f"Tracking: CSV has {len(self.rows)} rows, tracking has {len(entries)} entries")

        hashes = [e['row_hash'] for e in entries]
        if len(set(hashes)) != len(hashes):
            self.issues.append(f"Tracking: Hash collisions detected")

        self.stats['tracking_aligned'] = len(self.rows) == len(entries)

    def generate_report(self):
        """Generate validation report."""
        print(f"\nGenerating report...")

        REPORT_DIR.mkdir(parents=True, exist_ok=True)

        report = f"""# Discography Validation Report

**Generated**: {datetime.now().isoformat()}
**Total Rows**: {len(self.rows)}
**Schema Version**: v3.0 ({len(EXPECTED_ORDER)} columns)

---

## Executive Summary

"""

        if not self.issues:
            report += "All validations passed. The dataset is in excellent condition.\n\n"
        else:
            report += f"{len(self.issues)} issues found that need attention.\n\n"

        report += f"""## Schema Validation

- **Columns**: {len(self.fieldnames)}/{len(EXPECTED_ORDER)}
- **Column Order**: {'Correct' if self.stats.get('schema_valid') else 'Incorrect'}

## Data Completeness

"""

        for field, data in self.stats.get('completeness', {}).items():
            status = 'OK' if data['pct'] == 100 else f"{data['pct']:.1f}%"
            report += f"- **{field}**: {status} ({data['missing']} missing)\n"

        report += f"""
## URL Coverage

- **Spotify URLs**: {self.stats['url_coverage']['spotify']} ({self.stats['url_coverage']['spotify_pct']:.1f}%)
- **YouTube URLs**: {self.stats['url_coverage']['youtube']} ({self.stats['url_coverage']['youtube_pct']:.1f}%)
- **Any URL**: {self.stats['url_coverage']['any_url']} ({self.stats['url_coverage']['any_url_pct']:.1f}%)

### Coverage by Era

| Era | Coverage |
|-----|----------|
"""

        for era, data in self.stats['url_coverage']['by_era'].items():
            pct = data['spotify'] / data['total'] * 100 if data['total'] > 0 else 0
            report += f"| {era} | {data['spotify']}/{data['total']} ({pct:.1f}%) |\n"

        report += f"""
## Chart Data (1970-1985 Major Albums)

- **Total major albums**: {self.stats['chart_data']['major_albums_1970_1985']}
- **With chart position**: {self.stats['chart_data']['with_chart_position']} ({self.stats['chart_data']['chart_pct']:.1f}%)
- **With awards**: {self.stats['chart_data']['with_awards']}

## Issues Found

"""

        if self.issues:
            for issue in self.issues:
                report += f"- {issue}\n"
        else:
            report += "No issues found.\n"

        report += f"""
---

## Recommendations

"""

        if not self.issues:
            report += "The dataset is production-ready.\n"
        else:
            report += "Address the issues listed above, then re-run validation.\n"

        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write(report)

        print(f"   Report saved to {REPORT_FILE.name}")

        return report

    def run(self):
        """Run all validation checks."""
        print("DISCOGRAPHY VALIDATION")
        print("=" * 70)

        self.load_data()
        self.validate_schema()
        self.validate_data_types()
        self.validate_row_type_alignment()
        self.validate_completeness()
        self.validate_url_coverage()
        self.validate_chart_data()
        self.validate_tracking_alignment()

        report = self.generate_report()

        print("\n" + "=" * 70)
        print("VALIDATION COMPLETE")
        print("=" * 70)
        print(f"\nValidation finished.")
        print(f"   Total issues: {len(self.issues)}")
        print(f"   Report: {REPORT_FILE.name}")
        return len(self.issues) == 0

def main():
    validator = DiscographyValidator()
    success = validator.run()

    return 0 if success else 1

if __name__ == '__main__':
    exit(main())
