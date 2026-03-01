#!/usr/bin/env python3
"""Backfill duration_seconds from local Spotify cache only."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import DISCOGRAPHY_COLUMNS, safe_write_csv, validate_csv_input  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
CACHE_DIR = ROOT / "data" / ".spotify_cache"


def load_duration_map() -> dict[str, int]:
    duration_by_url: dict[str, int] = {}
    for cache_file in CACHE_DIR.glob("*.json"):
        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        tracks = payload.get("tracks")
        if not isinstance(tracks, dict):
            continue
        items = tracks.get("items", [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            url = item.get("external_urls", {}).get("spotify", "")
            dur_ms = item.get("duration_ms")
            if url and isinstance(dur_ms, int) and dur_ms > 0:
                duration_by_url[url] = round(dur_ms / 1000)
    return duration_by_url


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Backfill duration from Spotify cache")
    parser.add_argument("--sidecar", action="store_true", help="Write results to sidecar file instead of modifying discography.csv")
    args = parser.parse_args()

    duration_by_url = load_duration_map()
    rows, fieldnames = validate_csv_input(CSV_PATH, DISCOGRAPHY_COLUMNS, min_rows=1)

    if "duration_seconds" not in fieldnames:
        fieldnames.append("duration_seconds")

    sidecar_rows: list[dict[str, str]] = []
    updated = 0
    for idx, row in enumerate(rows):
        if row.get("duration_seconds", "").strip():
            continue
        spotify_url = row.get("spotify_url", "").strip()
        if not spotify_url.startswith("https://open.spotify.com/track/"):
            continue
        duration = duration_by_url.get(spotify_url)
        if duration:
            if args.sidecar:
                sidecar_rows.append({"row_index": str(idx), "field_name": "duration_seconds", "value": str(duration)})
            else:
                row["duration_seconds"] = str(duration)
            updated += 1

    if args.sidecar:
        sidecar_dir = ROOT / "data" / ".enrich_sidecars"
        sidecar_dir.mkdir(parents=True, exist_ok=True)
        safe_write_csv(
            sidecar_dir / "backfill_duration.csv",
            sidecar_rows,
            ["row_index", "field_name", "value"],
            backup=False,
        )
    else:
        safe_write_csv(CSV_PATH, rows, fieldnames)

    print(f"updated_duration_seconds={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
