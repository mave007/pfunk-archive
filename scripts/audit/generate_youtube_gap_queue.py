#!/usr/bin/env python3
"""Generate prioritized queue for missing YouTube URLs."""

from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
OUT_CSV = ROOT / "reports" / "research_queue" / "youtube_missing_priority.csv"

ERA_WEIGHT = {
    "Classic P-Funk (1970–1981)": 4,
    "Comeback Era (1993–2004)": 3,
    "Transition Era (1982–1992)": 3,
    "Legacy Era (2016–present)": 2,
    "Late Career (2005–2015)": 2,
    "Pre-P-Funk (1955–1969)": 2,
}

ROW_TYPE_WEIGHT = {"album": 4, "single": 3, "track": 1}


def priority(row: dict[str, str]) -> float:
    base = 0.0
    era = ERA_WEIGHT.get(row.get("era", ""), 1)
    row_type = ROW_TYPE_WEIGHT.get(row.get("row_type", ""), 1)
    spotify_bonus = 1.5 if row.get("spotify_url", "").strip() else 0.0
    return round(base + era + row_type + spotify_bonus, 2)


def main() -> int:
    with CSV_PATH.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    queue = []
    for row in rows:
        if row.get("youtube_url", "").strip():
            continue
        entry = {
            "priority_score": priority(row),
            "artist": row.get("artist", ""),
            "song_name": row.get("song_name", ""),
            "album_name": row.get("album_name", ""),
            "row_type": row.get("row_type", ""),
            "release_date": row.get("release_date", ""),
            "era": row.get("era", ""),
            "spotify_url": row.get("spotify_url", ""),
        }
        queue.append(entry)

    queue.sort(key=lambda item: item["priority_score"], reverse=True)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "priority_score",
        "artist",
        "song_name",
        "album_name",
        "row_type",
        "release_date",
        "era",
        "spotify_url",
    ]
    with OUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(queue)

    print(f"youtube_missing_rows={len(queue)}")
    print(f"queue_file={OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
