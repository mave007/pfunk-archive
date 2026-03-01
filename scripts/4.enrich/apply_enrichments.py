#!/usr/bin/env python3
"""Apply enrichment sidecar files to discography.csv in one atomic pass.

Sidecar files are CSVs in data/.enrich_sidecars/ with columns:
  row_index, field_name, value

Each enrichment script can write its results to a sidecar file instead of
modifying discography.csv directly.  This script reads all sidecars,
applies the changes, and writes the result atomically.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import safe_write_csv  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
SIDECAR_DIR = ROOT / "data" / ".enrich_sidecars"

SIDECAR_FIELDS = ["row_index", "field_name", "value"]


def load_discography() -> tuple[list[dict[str, str]], list[str]]:
    with CSV_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader), list(reader.fieldnames or [])


def load_sidecars() -> list[tuple[str, list[dict[str, str]]]]:
    """Return list of (filename, rows) for each sidecar CSV."""
    if not SIDECAR_DIR.exists():
        return []
    results = []
    for path in sorted(SIDECAR_DIR.glob("*.csv")):
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        results.append((path.name, rows))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply enrichment sidecars to discography")
    parser.add_argument("--write", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument("--clean", action="store_true", help="Remove sidecar files after applying")
    args = parser.parse_args()

    rows, fieldnames = load_discography()
    sidecars = load_sidecars()

    if not sidecars:
        print("No sidecar files found in data/.enrich_sidecars/")
        return 0

    total_applied = 0
    total_skipped = 0

    for filename, sidecar_rows in sidecars:
        applied = 0
        skipped = 0
        for sr in sidecar_rows:
            try:
                idx = int(sr.get("row_index", -1))
            except (ValueError, TypeError):
                skipped += 1
                continue
            field_name = sr.get("field_name", "").strip()
            value = sr.get("value", "").strip()

            if idx < 0 or idx >= len(rows):
                skipped += 1
                continue
            if field_name not in fieldnames:
                skipped += 1
                continue
            if not value:
                skipped += 1
                continue

            current = rows[idx].get(field_name, "").strip()
            if current:
                skipped += 1
                continue

            rows[idx][field_name] = value
            applied += 1

        print(f"  {filename}: applied={applied}, skipped={skipped}")
        total_applied += applied
        total_skipped += skipped

    if args.write and total_applied > 0:
        safe_write_csv(CSV_PATH, rows, fieldnames)
        print(f"Wrote {total_applied} enrichments to discography.csv")
        if args.clean:
            for filename, _ in sidecars:
                (SIDECAR_DIR / filename).unlink(missing_ok=True)
            print("Cleaned sidecar files")
    else:
        print(f"Dry-run: would apply {total_applied} enrichments (use --write)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
