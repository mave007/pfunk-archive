#!/usr/bin/env python3
"""Generate prioritized queue for medium-severity Spotify URL mismatches."""

from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
AUDIT_JSON = ROOT / "reports" / "spotify_audit" / "mismatch_latest.json"
OUT_CSV = ROOT / "reports" / "spotify_audit" / "medium_priority_queue.csv"

ERA_WEIGHT = {
    "Classic P-Funk (1970–1981)": 4,
    "Comeback Era (1993–2004)": 3,
    "Transition Era (1982–1992)": 3,
    "Legacy Era (2016–present)": 2,
    "Late Career (2005–2015)": 2,
    "Pre-P-Funk (1955–1969)": 2,
}

ROW_TYPE_WEIGHT = {"album": 4, "single": 3, "track": 2}


def build_row_index() -> dict[int, dict[str, str]]:
    with CSV_PATH.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return {idx: row for idx, row in enumerate(rows, start=2)}


def is_core_act(artist: str) -> bool:
    artist_norm = artist.strip().lower()
    core_tokens = ("parliament", "funkadelic", "george clinton", "bootsy", "parliaments")
    return any(token in artist_norm for token in core_tokens)


def impact_score(base_row: dict[str, str], finding: dict) -> float:
    base = 0.0
    era = ERA_WEIGHT.get(base_row.get("era", ""), 1)
    row_type = ROW_TYPE_WEIGHT.get((finding.get("row_type") or "").strip(), 1)
    core_bonus = 2 if is_core_act(base_row.get("artist", "")) else 0
    return round(base + era + row_type + core_bonus, 4)


def mismatch_score(finding: dict) -> float:
    title = float(finding.get("title_score", 0.0) or 0.0)
    artist = float(finding.get("artist_score", 0.0) or 0.0)
    type_penalty = 2.5 if bool(finding.get("type_mismatch")) else 0.0
    low_artist_penalty = 1.5 if "low_artist_similarity" in (finding.get("reason_codes") or []) else 0.0
    return round((1.0 - title) * 3.0 + (1.0 - artist) * 2.0 + type_penalty + low_artist_penalty, 4)


def review_action(finding: dict) -> str:
    reasons = set(finding.get("reason_codes") or [])
    artist_score = float(finding.get("artist_score", 0.0) or 0.0)
    if "low_artist_similarity" in reasons:
        return "verify_artist_match_first"
    if "low_title_similarity" in reasons and artist_score >= 0.85:
        return "check_alt_title_or_version"
    return "manual_track_search"


def main() -> int:
    row_index = build_row_index()
    payload = json.loads(AUDIT_JSON.read_text(encoding="utf-8"))
    findings = payload.get("findings", [])

    queue: list[dict[str, str | int | float]] = []
    for finding in findings:
        if finding.get("severity") != "medium":
            continue
        row_number = int(finding.get("row_number", 0) or 0)
        if row_number <= 1:
            continue
        base_row = row_index.get(row_number, {})
        impact = impact_score(base_row, finding)
        mismatch = mismatch_score(finding)
        priority = round(impact * 0.65 + mismatch * 0.35, 4)
        queue.append(
            {
                "priority_score": priority,
                "impact_score": impact,
                "mismatch_score": mismatch,
                "row_number": row_number,
                "row_type": finding.get("row_type", ""),
                "artist": finding.get("artist", ""),
                "song_name": finding.get("song_name", ""),
                "album_name": finding.get("album_name", ""),
                "release_date": base_row.get("release_date", ""),
                "era": base_row.get("era", ""),
                "title_score": finding.get("title_score", 0.0),
                "artist_score": finding.get("artist_score", 0.0),
                "reason_codes": "|".join(finding.get("reason_codes", [])),
                "review_action": review_action(finding),
                "spotify_url": finding.get("spotify_url", ""),
                "spotify_title": finding.get("spotify_title", ""),
                "spotify_artists": "|".join(finding.get("spotify_artists", [])),
            }
        )

    queue.sort(
        key=lambda item: (
            float(item["priority_score"]),
            -int(item["row_number"]),
        ),
        reverse=True,
    )
    for idx, item in enumerate(queue, start=1):
        item["priority_rank"] = idx

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "priority_rank",
        "priority_score",
        "impact_score",
        "mismatch_score",
        "row_number",
        "row_type",
        "artist",
        "song_name",
        "album_name",
        "release_date",
        "era",
        "title_score",
        "artist_score",
        "reason_codes",
        "review_action",
        "spotify_url",
        "spotify_title",
        "spotify_artists",
    ]
    with OUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(queue)

    print(f"medium_rows={len(queue)}")
    print(f"queue_file={OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
