#!/usr/bin/env python3
"""Reconcile url_search_log.json with canonical discography rows."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
TRACKING_PATH = ROOT / "data" / "url_search_log.json"


def stable_signature(row: dict[str, str]) -> str:
    parts = [
        row.get("artist", "").strip().lower(),
        row.get("album_name", "").strip().lower(),
        row.get("song_name", "").strip().lower(),
        row.get("track_position", "").strip().lower(),
        row.get("release_date", "").strip().lower(),
        row.get("row_type", "").strip().lower(),
        row.get("release_category", "").strip().lower(),
        row.get("edition_type", "").strip().lower(),
    ]
    return "|".join(parts)


def stable_hash(signature: str) -> str:
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]


def load_csv_rows() -> list[dict[str, str]]:
    with CSV_PATH.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_tracking() -> dict[str, Any]:
    if not TRACKING_PATH.exists():
        return {"entries": []}
    with TRACKING_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_history(entry: dict[str, Any]) -> list[dict[str, Any]]:
    history = entry.get("search_history", [])
    if isinstance(history, list):
        return [item for item in history if isinstance(item, dict)]
    return []


def best_entry(current: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    if current is None:
        return candidate
    return candidate if len(normalize_history(candidate)) > len(normalize_history(current)) else current


def index_existing(entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for entry in entries:
        full_signature_parts = [
            str(entry.get("artist", "")).strip().lower(),
            str(entry.get("album_name", "")).strip().lower(),
            str(entry.get("song_name", "")).strip().lower(),
            str(entry.get("track_position", "")).strip().lower(),
            str(entry.get("release_date", "")).strip().lower(),
            str(entry.get("row_type", "")).strip().lower(),
            str(entry.get("release_category", "")).strip().lower(),
            str(entry.get("edition_type", "")).strip().lower(),
        ]
        full_signature = "|".join(full_signature_parts)
        legacy_signature = "|".join(full_signature_parts[:5])
        index[full_signature] = best_entry(index.get(full_signature), entry)
        index[legacy_signature] = best_entry(index.get(legacy_signature), entry)
    return index


def infer_status(url_value: str, existing_status: str | None) -> str:
    if url_value.strip():
        return "found"
    if existing_status in {"not_found", "needs_research", "not_searched"}:
        return existing_status
    return "not_searched"


def infer_confidence(status: str, existing_confidence: str | None) -> str:
    if existing_confidence in {"none", "low", "medium", "high"}:
        return existing_confidence
    if status == "found":
        return "low"
    if status == "needs_research":
        return "medium"
    return "none"


def reconcile(rows: list[dict[str, str]], existing: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    indexed = index_existing(existing.get("entries", []))
    entries = []

    stats = {
        "matched_existing_entries": 0,
        "new_entries": 0,
        "total_rows": len(rows),
    }

    # row_number is CSV line number (header is line 1, first row is line 2)
    for idx, row in enumerate(rows, start=2):
        signature = stable_signature(row)
        row_hash = stable_hash(signature)
        legacy_signature = "|".join(signature.split("|")[:5])
        prev = indexed.get(signature) or indexed.get(legacy_signature)
        if prev:
            stats["matched_existing_entries"] += 1
        else:
            stats["new_entries"] += 1

        spotify_status = infer_status(row.get("spotify_url", ""), (prev or {}).get("spotify_status"))
        youtube_status = infer_status(row.get("youtube_url", ""), (prev or {}).get("youtube_status"))
        confidence = infer_confidence(spotify_status, (prev or {}).get("confidence_level"))
        history = normalize_history(prev or {})

        entry = {
            "row_hash": row_hash,
            "row_number": idx,
            "artist": row.get("artist", ""),
            "album_name": row.get("album_name", ""),
            "song_name": row.get("song_name", ""),
            "track_position": row.get("track_position", ""),
            "release_date": row.get("release_date", ""),
            "row_type": row.get("row_type", ""),
            "release_category": row.get("release_category", ""),
            "edition_type": row.get("edition_type", ""),
            "spotify_status": spotify_status,
            "youtube_status": youtube_status,
            "last_search_date": (prev or {}).get("last_search_date"),
            "search_attempts": int((prev or {}).get("search_attempts", 0) or 0),
            "confidence_level": confidence,
            "search_history": history,
        }
        entries.append(entry)

    found_spotify = sum(1 for entry in entries if entry["spotify_status"] == "found")
    found_youtube = sum(1 for entry in entries if entry["youtube_status"] == "found")
    needs_research = sum(1 for entry in entries if entry["spotify_status"] == "needs_research")
    not_searched = sum(1 for entry in entries if entry["spotify_status"] == "not_searched")

    payload = {
        "version": "2.0",
        "created": existing.get("created") or datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "schema": {
            "row_hash": "SHA256 hash of stable row signature (first 16 chars)",
            "spotify_status": "found|not_found|needs_research|not_searched",
            "youtube_status": "found|not_found|needs_research|not_searched",
            "confidence_level": "none|low|medium|high",
            "search_attempts": "Number of search attempts made",
            "search_history": "Array of search attempts with date, method, result",
        },
        "statistics": {
            "total": len(entries),
            "has_spotify_url": found_spotify,
            "has_youtube_url": found_youtube,
            "needs_research": needs_research,
            "not_searched": not_searched,
            "total_entries": len(entries),
        },
        "entries": entries,
    }

    return payload, stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile URL tracking file with discography rows")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = load_csv_rows()
    existing = load_tracking()
    payload, stats = reconcile(rows, existing)

    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stats": stats,
        "tracking_total": payload["statistics"]["total"],
    }
    args.report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if args.write:
        TRACKING_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"rows={stats['total_rows']}")
    print(f"matched_existing={stats['matched_existing_entries']}")
    print(f"new_entries={stats['new_entries']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
