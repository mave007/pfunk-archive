#!/usr/bin/env python3
"""
Schema and field validation for P-Funk discography CSV.
Validates structural integrity, data completeness, and URL coverage.
"""

import csv
import re
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import (  # noqa: E402
    DISCOGRAPHY_COLUMNS as EXPECTED_FIELDS,
    VALID_ERAS,
    VALID_GENRES,
    VALID_RELEASE_CATEGORIES,
)

CSV_FILE = Path(__file__).parent.parent.parent / "data" / "discography.csv"

class SchemaValidator:
    def __init__(self):
        self.rows = []
        self.issues = defaultdict(list)
        self.stats = defaultdict(int)

    def load_csv(self):
        """Load and validate CSV structure."""
        print("Loading CSV...")
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            if reader.fieldnames != EXPECTED_FIELDS:
                self.issues['CRITICAL'].append(
                    f"CSV header mismatch. Expected {len(EXPECTED_FIELDS)} columns, "
                    f"got {len(reader.fieldnames)}"
                )
                print(f"Expected: {EXPECTED_FIELDS}")
                print(f"Got: {reader.fieldnames}")
                return False

            for idx, row in enumerate(reader, start=2):
                row['_line'] = idx
                self.rows.append(row)

        self.stats['total_rows'] = len(self.rows)
        print(f"Loaded {len(self.rows)} rows\n")
        return True

    def validate_structural_integrity(self):
        """Validate overall CSV structure, field discipline, and data completeness."""
        print("\n" + "="*80)
        print("STRUCTURAL INTEGRITY AND DATA COMPLETENESS")
        print("="*80)

        empty_artist = []
        missing_era = 0
        missing_genre = 0
        invalid_era = []
        invalid_genre = []
        rows_with_spotify = 0
        rows_with_youtube = 0
        invalid_spotify_urls = []
        invalid_youtube_urls = []

        for row in self.rows:
            if not row.get('artist'):
                empty_artist.append(row['_line'])

            valid_row_types = ['album', 'track', 'single']
            if row.get('row_type') and row['row_type'] not in valid_row_types:
                self.issues['row_type'].append(f"Line {row['_line']}: Invalid row_type '{row['row_type']}'")

            if row.get('release_category') and row['release_category'] not in VALID_RELEASE_CATEGORIES:
                self.issues['release_category'].append(
                    f"Line {row['_line']}: Invalid release_category '{row['release_category']}'"
                )

            era_val = row.get('era', '')
            genre_val = row.get('genre', '')
            if not era_val:
                missing_era += 1
            elif era_val not in VALID_ERAS:
                if re.search(r'\d+/\d+', era_val):
                    invalid_era.append((row['_line'], row.get('song_name', ''), era_val))
                else:
                    invalid_era.append((row['_line'], row.get('song_name', ''), era_val))

            if not genre_val:
                missing_genre += 1
            else:
                genre_terms = [g.strip() for g in genre_val.split(',')]
                invalid_terms = [
                    g for g in genre_terms
                    if g not in VALID_GENRES and not g.endswith('(Compilation)') and not g.endswith('(Live)')
                ]
                if invalid_terms:
                    invalid_genre.append((row['_line'], row.get('song_name', ''), ', '.join(invalid_terms)))

            spotify_url = row.get('spotify_url', '')
            youtube_url = row.get('youtube_url', '')
            if spotify_url:
                rows_with_spotify += 1
                if not spotify_url.startswith('https://open.spotify.com/'):
                    invalid_spotify_urls.append((row['_line'], row.get('song_name', ''), spotify_url))
            if youtube_url:
                rows_with_youtube += 1
                if not youtube_url.startswith('https://www.youtube.com/') and not youtube_url.startswith('https://youtu.be/'):
                    invalid_youtube_urls.append((row['_line'], row.get('song_name', ''), youtube_url))

        self.stats['missing_era'] = missing_era
        self.stats['missing_genre'] = missing_genre
        self.stats['invalid_era'] = len(invalid_era)
        self.stats['invalid_genre'] = len(invalid_genre)
        self.stats['rows_with_spotify'] = rows_with_spotify
        self.stats['rows_with_youtube'] = rows_with_youtube

        print(f"\nEmpty artist field: {len(empty_artist)}")
        print(f"Missing era: {missing_era}")
        print(f"Missing genre: {missing_genre}")
        print(f"Invalid era values: {len(invalid_era)}")
        print(f"Invalid genre terms: {len(invalid_genre)}")
        print(f"Rows with spotify_url: {rows_with_spotify}")
        print(f"Rows with youtube_url: {rows_with_youtube}")
        print(f"Invalid Spotify URLs: {len(invalid_spotify_urls)}")
        print(f"Invalid YouTube URLs: {len(invalid_youtube_urls)}")

        if empty_artist:
            self.issues['CRITICAL'].extend([f"Line {line}: Empty artist field" for line in empty_artist[:5]])

        for line, song, url in invalid_spotify_urls[:5]:
            self.issues['URLs'].append(f"Line {line}: Invalid Spotify URL for '{song}': {url}")
        for line, song, url in invalid_youtube_urls[:5]:
            self.issues['URLs'].append(f"Line {line}: Invalid YouTube URL for '{song}': {url}")

        print("\nOriginal release diagnostics (non-blocking):")
        originals_by_recording = defaultdict(list)
        for row in self.rows:
            if row.get('release_category') == 'original':
                key = (
                    row.get('artist', ''),
                    row.get('song_name', ''),
                    row.get('version_type', ''),
                )
                originals_by_recording[key].append(row['_line'])

        duplicate_originals = {k: v for k, v in originals_by_recording.items() if len(v) > 1}
        self.stats['original_releases'] = sum(1 for row in self.rows if row.get('release_category') == 'original')
        self.stats['duplicate_originals'] = len(duplicate_originals)

        print(f"  Total original releases marked: {self.stats['original_releases']}")
        print(f"  Recordings with multiple originals: {len(duplicate_originals)}")

        if duplicate_originals:
            print(f"  Sample duplicate originals:")
            for (artist, title, version_type), lines in list(duplicate_originals.items())[:3]:
                label = version_type or "unknown"
                print(f"    {artist} - {title} ({label}): lines {lines}")
                self.issues['WARNING'].append(
                    f"Duplicate original designation for {artist} - {title} ({label}): lines {lines}"
                )

        if len(empty_artist) == 0:
            print(f"\n  STRUCTURAL INTEGRITY: PASS")
        else:
            print(f"\n  STRUCTURAL INTEGRITY: ISSUES FOUND")

    def generate_report(self):
        """Generate final validation report."""
        print("\n" + "="*80)
        print("FINAL VALIDATION REPORT")
        print("="*80)

        total = self.stats['total_rows']
        print(f"\nTotal rows: {total}")
        print(f"Original releases: {self.stats['original_releases']}")

        print("\nData Completeness:")
        era_pct = (1 - self.stats['missing_era'] / total) * 100 if total > 0 else 0
        genre_pct = (1 - self.stats['missing_genre'] / total) * 100 if total > 0 else 0
        spotify_pct = (self.stats['rows_with_spotify'] / total * 100) if total > 0 else 0
        youtube_pct = (self.stats['rows_with_youtube'] / total * 100) if total > 0 else 0
        print(f"  Rows with era: {total - self.stats['missing_era']} ({era_pct:.1f}%)")
        print(f"  Rows with genre: {total - self.stats['missing_genre']} ({genre_pct:.1f}%)")
        print(f"  Rows with Spotify URLs: {self.stats['rows_with_spotify']} ({spotify_pct:.1f}%)")
        print(f"  Rows with YouTube URLs: {self.stats['rows_with_youtube']} ({youtube_pct:.1f}%)")

        if self.issues:
            print("\n" + "="*80)
            print("ISSUES FOUND")
            print("="*80)
            for category, issue_list in sorted(self.issues.items()):
                if issue_list:
                    print(f"\n{category}:")
                    for issue in issue_list[:10]:
                        print(f"  - {issue}")
                    if len(issue_list) > 10:
                        print(f"  ... and {len(issue_list) - 10} more issues")
        else:
            print("\nNO ISSUES FOUND - Validation successful!")

        print("\n" + "="*80)
        critical_issues = len(self.issues.get('CRITICAL', []))
        total_issues = sum(len(v) for v in self.issues.values())

        if critical_issues > 0:
            print("VALIDATION FAILED - Critical issues found")
        elif total_issues > 20:
            print("VALIDATION PARTIAL - Multiple issues need attention")
        elif total_issues > 0:
            print("VALIDATION MOSTLY PASS - Minor issues found")
        else:
            print("VALIDATION PASSED - All checks complete and correct!")

        print("="*80)

def main():
    validator = SchemaValidator()

    if not validator.load_csv():
        print("Failed to load CSV. Exiting.")
        return 1

    validator.validate_structural_integrity()
    validator.generate_report()

    critical_issues = len(validator.issues.get('CRITICAL', []))
    return 1 if critical_issues > 0 else 0

if __name__ == '__main__':
    exit(main())
