#!/usr/bin/env python3
"""Spotify enrichment with cache, retry, and checkpoints."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import spotipy
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import (  # noqa: E402
    DISCOGRAPHY_COLUMNS, ProgressTracker, safe_write_csv,
    load_env, require_env, validate_csv_input,
)

load_env()

ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
TRACKING_PATH = ROOT / "data" / "url_search_log.json"
CACHE_DIR = ROOT / "data" / ".spotify_cache"
CHECKPOINT_PATH = ROOT / "data" / ".enrich_spotify_checkpoint.json"

CACHE_DIR.mkdir(parents=True, exist_ok=True)


def cache_key(namespace: str, query: str) -> str:
    raw = f"{namespace}:{query.strip().lower()}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def cache_load(key: str) -> dict[str, Any] | None:
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def cache_save(key: str, payload: dict[str, Any]) -> None:
    path = CACHE_DIR / f"{key}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def normalize_text(value: str) -> str:
    value = value or ""
    value = re.sub(r"\(feat\.[^)]+\)", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\(ft\.[^)]+\)", "", value, flags=re.IGNORECASE)
    return value.strip()


class SpotifyEnricher:
    def __init__(self, retry_limit: int = 3, sleep_seconds: float = 0.3):
        client_id = require_env("SPOTIPY_CLIENT_ID")
        client_secret = require_env("SPOTIPY_CLIENT_SECRET")
        self.spotify = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret,
            )
        )
        self.retry_limit = retry_limit
        self.sleep_seconds = sleep_seconds

    def search_with_backoff(self, query: str, item_type: str) -> dict[str, Any]:
        key = cache_key(item_type, query)
        cached = cache_load(key)
        if cached is not None:
            return cached

        delay = self.sleep_seconds
        for attempt in range(self.retry_limit + 1):
            try:
                response = self.spotify.search(q=query, type=item_type, limit=5)
                cache_save(key, response)
                time.sleep(self.sleep_seconds)
                return response
            except SpotifyException as exc:
                if exc.http_status == 400:
                    cache_save(key, {"tracks": {"items": []}, "albums": {"items": []}})
                    return {}
                if exc.http_status == 429 and attempt < self.retry_limit:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise
            except Exception:
                if attempt < self.retry_limit:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise
        return {}

    def search_track(self, artist: str, song_name: str, album_name: str) -> str | None:
        song = normalize_text(song_name)
        album = normalize_text(album_name)
        queries = [
            f'track:"{song}" artist:"{artist}" album:"{album}"',
            f'track:"{song}" artist:"{artist}"',
            f'"{song}" "{artist}"',
        ]
        for query in queries:
            results = self.search_with_backoff(query=query, item_type="track")
            items = results.get("tracks", {}).get("items", [])
            if items:
                return items[0]["external_urls"]["spotify"]
        return None

    def search_album(self, artist: str, album_name: str) -> str | None:
        album = normalize_text(album_name)
        queries = [
            f'album:"{album}" artist:"{artist}"',
            f'"{album}" "{artist}"',
        ]
        for query in queries:
            results = self.search_with_backoff(query=query, item_type="album")
            items = results.get("albums", {}).get("items", [])
            if items:
                return items[0]["external_urls"]["spotify"]
        return None


def load_rows() -> tuple[list[dict[str, str]], list[str]]:
    return validate_csv_input(CSV_PATH, DISCOGRAPHY_COLUMNS, min_rows=1)


def save_rows(rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    safe_write_csv(CSV_PATH, rows, fieldnames, expected_columns=DISCOGRAPHY_COLUMNS)


def load_checkpoint() -> dict[str, Any]:
    if not CHECKPOINT_PATH.exists():
        return {}
    try:
        return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_checkpoint(payload: dict[str, Any]) -> None:
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    CHECKPOINT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def process_rows(
    enricher: SpotifyEnricher,
    rows: list[dict[str, str]],
    start_index: int,
    limit: int | None,
) -> tuple[int, int]:
    searched = 0
    found = 0

    end_index = len(rows) if limit is None else min(len(rows), start_index + limit)
    total_to_scan = end_index - start_index
    progress = ProgressTracker(total=total_to_scan, noun="rows")
    for idx in range(start_index, end_index):
        row = rows[idx]
        if row.get("spotify_url", "").strip():
            progress.update(extra=f"| skipped (has URL)")
            continue

        row_type = row.get("row_type", "").strip()
        artist = row.get("artist", "")
        song_name = row.get("song_name", "")
        album_name = row.get("album_name", "")

        url = None
        if row_type == "album":
            url = enricher.search_album(artist=artist, album_name=album_name)
        else:
            url = enricher.search_track(artist=artist, song_name=song_name, album_name=album_name)

        searched += 1
        if url:
            row["spotify_url"] = url
            found += 1

        if searched and searched % 50 == 0:
            save_checkpoint({"last_index": idx, "searched": searched, "found": found})

        progress.update(extra=f"| searched={searched} found={found}")
    progress.finish(extra=f"| searched={searched} found={found}")

    return searched, found


def process_needs_research(
    enricher: SpotifyEnricher,
    rows: list[dict[str, str]],
    tracking: dict[str, Any],
) -> tuple[int, int]:
    entries = tracking.get("entries", [])
    needs_research = [e for e in entries if e.get("spotify_status") == "needs_research"]
    searched = 0
    found = 0
    progress = ProgressTracker(total=len(needs_research), noun="entries")
    for entry in entries:
        if entry.get("spotify_status") != "needs_research":
            continue
        row_num = int(entry.get("row_number", 0))
        if row_num <= 0 or row_num > len(rows):
            continue
        row = rows[row_num - 1]
        if row.get("spotify_url", "").strip():
            continue
        url = enricher.search_track(
            artist=row.get("artist", ""),
            song_name=row.get("song_name", ""),
            album_name=row.get("album_name", ""),
        )
        searched += 1
        if url:
            row["spotify_url"] = url
            entry["spotify_status"] = "found"
            entry["last_search_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            entry["search_attempts"] = int(entry.get("search_attempts", 0) or 0) + 1
            history = entry.get("search_history", [])
            if not isinstance(history, list):
                history = []
            history.append(
                {
                    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "round": "Spotify Enrichment",
                    "method": "cache_first_backoff_search",
                    "result": "found",
                }
            )
            entry["search_history"] = history
            found += 1

        progress.update(extra=f"| searched={searched} found={found}")
    progress.finish(extra=f"| searched={searched} found={found}")

    return searched, found


def load_tracking() -> dict[str, Any]:
    if not TRACKING_PATH.exists():
        return {}
    return json.loads(TRACKING_PATH.read_text(encoding="utf-8"))


def save_tracking(payload: dict[str, Any]) -> None:
    payload["last_updated"] = datetime.now(timezone.utc).isoformat()
    TRACKING_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spotify enrichment runner")
    parser.add_argument("--mode", choices=["full", "batch", "research"], default="full")
    parser.add_argument("--start", type=int, default=0, help="Start index for batch mode")
    parser.add_argument("--limit", type=int, default=None, help="Row limit for batch mode")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows, fieldnames = load_rows()
    checkpoint = load_checkpoint()
    enricher = SpotifyEnricher()
    checkpoint_last_index = int(checkpoint.get("last_index", args.start))

    if args.mode == "research":
        tracking = load_tracking()
        searched, found = process_needs_research(enricher=enricher, rows=rows, tracking=tracking)
        save_rows(rows, fieldnames)
        save_tracking(tracking)
        checkpoint_last_index = int(checkpoint.get("last_index", args.start))
    else:
        start = args.start
        if args.mode == "full" and checkpoint.get("last_index") is not None:
            start = int(checkpoint["last_index"]) + 1
        searched, found = process_rows(
            enricher=enricher,
            rows=rows,
            start_index=start,
            limit=args.limit,
        )
        save_rows(rows, fieldnames)
        checkpoint_last_index = start + searched - 1 if searched else start

    save_checkpoint(
        {
            "mode": args.mode,
            "last_index": checkpoint_last_index,
            "searched": searched,
            "found": found,
        }
    )
    print(f"mode={args.mode}")
    print(f"searched={searched}")
    print(f"found={found}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
