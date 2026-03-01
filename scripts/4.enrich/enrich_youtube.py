#!/usr/bin/env python3
"""Enrich missing YouTube URLs using YouTube Data API v3 with cache and batch mode."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import sys

from googleapiclient.discovery import build as build_youtube_service
from googleapiclient.errors import HttpError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import (  # noqa: E402
    DISCOGRAPHY_COLUMNS, ProgressTracker, safe_write_csv,
    load_env, require_env, validate_csv_input,
)

load_env()

ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
CACHE_DIR = ROOT / "data" / ".youtube_cache"
CHECKPOINT_PATH = ROOT / "data" / ".youtube_enrich_checkpoint.json"

CACHE_DIR.mkdir(parents=True, exist_ok=True)

ERA_WEIGHT = {
    "Classic P-Funk (1970\u20131981)": 4,
    "Comeback Era (1993\u20132004)": 3,
    "Transition Era (1982\u20131992)": 3,
    "Legacy Era (2016\u2013present)": 2,
    "Late Career (2005\u20132015)": 2,
    "Pre-P-Funk (1955\u20131969)": 2,
}

ROW_TYPE_WEIGHT = {"album": 4, "single": 3, "track": 1}


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


def norm(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"\((feat\.|ft\.|featuring)\s+[^)]*\)", "", text, flags=re.IGNORECASE)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, norm(left), norm(right)).ratio()


def priority_score(row: dict[str, str]) -> float:
    era = ERA_WEIGHT.get(row.get("era", ""), 1)
    row_type = ROW_TYPE_WEIGHT.get(row.get("row_type", ""), 1)
    spotify_bonus = 1.5 if row.get("spotify_url", "").strip() else 0.0
    return round(era + row_type + spotify_bonus, 2)


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


class YouTubeEnricher:
    def __init__(self, api_key: str, sleep_seconds: float = 0.3):
        self.youtube = build_youtube_service("youtube", "v3", developerKey=api_key)
        self.sleep_seconds = sleep_seconds
        self.api_calls = 0

    def search(self, query: str, search_type: str = "video", max_results: int = 5) -> dict[str, Any]:
        key = cache_key(f"yt_{search_type}", query)
        cached = cache_load(key)
        if cached is not None:
            return cached

        try:
            response = (
                self.youtube.search()
                .list(q=query, type=search_type, part="snippet", maxResults=max_results)
                .execute()
            )
            cache_save(key, response)
            self.api_calls += 1
            time.sleep(self.sleep_seconds)
            return response
        except HttpError as exc:
            if exc.resp.status == 403:
                print(f"API quota exceeded after {self.api_calls} calls")
                raise
            cache_save(key, {"items": [], "error": str(exc)})
            return {"items": []}

    def _extract_url(self, item: dict[str, Any]) -> str:
        kind = item.get("id", {}).get("kind", "")
        vid = item.get("id", {}).get("videoId", "")
        pid = item.get("id", {}).get("playlistId", "")
        if "video" in kind and vid:
            return f"https://www.youtube.com/watch?v={vid}"
        if "playlist" in kind and pid:
            return f"https://www.youtube.com/playlist?list={pid}"
        return ""

    def _score_result(self, item: dict[str, Any], artist: str, title: str) -> float:
        snippet = item.get("snippet", {})
        yt_title = snippet.get("title", "")
        channel = snippet.get("channelTitle", "")

        title_sim = similarity(title, yt_title)
        artist_in_title = 0.15 if norm(artist) in norm(yt_title) else 0.0
        channel_sim = similarity(artist, channel) * 0.2

        official_bonus = 0.0
        channel_lower = channel.lower()
        title_lower = yt_title.lower()
        if any(kw in channel_lower for kw in ("official", "vevo", "topic")):
            official_bonus = 0.1
        if "official" in title_lower:
            official_bonus = max(official_bonus, 0.05)

        return min(title_sim * 0.55 + artist_in_title + channel_sim + official_bonus, 1.0)

    def search_track(self, artist: str, song_name: str, album_name: str) -> str | None:
        queries = [
            f'"{song_name}" "{artist}" official',
            f'"{song_name}" "{artist}"',
        ]
        best_url = ""
        best_score = 0.0

        for query in queries:
            response = self.search(query, search_type="video")
            for item in response.get("items", []):
                url = self._extract_url(item)
                if not url:
                    continue
                score = self._score_result(item, artist, song_name)
                if score > best_score:
                    best_score = score
                    best_url = url

        return best_url if best_score >= 0.55 else None

    def search_album(self, artist: str, album_name: str) -> str | None:
        queries = [
            f'"{album_name}" "{artist}" full album',
            f'"{album_name}" "{artist}" playlist',
        ]
        best_url = ""
        best_score = 0.0

        for query in queries:
            for stype in ("playlist", "video"):
                response = self.search(query, search_type=stype)
                for item in response.get("items", []):
                    url = self._extract_url(item)
                    if not url:
                        continue
                    score = self._score_result(item, artist, album_name)
                    if score > best_score:
                        best_score = score
                        best_url = url

        return best_url if best_score >= 0.50 else None


def build_priority_queue(rows: list[dict[str, str]]) -> list[int]:
    candidates = []
    for idx, row in enumerate(rows):
        if row.get("youtube_url", "").strip():
            continue
        candidates.append((priority_score(row), idx))
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return [idx for _, idx in candidates]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="YouTube URL enrichment")
    parser.add_argument("--limit", type=int, default=95, help="Max API searches per run (default 95)")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = require_env("YOUTUBE_API_KEY")
    enricher = YouTubeEnricher(api_key=api_key)

    rows, fieldnames = load_rows()
    checkpoint = load_checkpoint() if args.resume else {}
    completed_indices: set[int] = set(checkpoint.get("completed_indices", []))

    queue = build_priority_queue(rows)
    effective_total = min(len(queue) - len(completed_indices), args.limit)
    searched = 0
    found = 0
    skipped_cache = 0

    progress = ProgressTracker(total=max(effective_total, 1), noun="searches")
    for idx in queue:
        if idx in completed_indices:
            continue
        if enricher.api_calls >= args.limit:
            break

        row = rows[idx]
        row_type = row.get("row_type", "").strip()
        artist = row.get("artist", "")
        song_name = row.get("song_name", "")
        album_name = row.get("album_name", "")

        url = None
        try:
            if row_type == "album":
                url = enricher.search_album(artist=artist, album_name=album_name)
            else:
                url = enricher.search_track(artist=artist, song_name=song_name, album_name=album_name)
        except HttpError as exc:
            if exc.resp.status == 403:
                print(f"Quota exceeded at row {idx}, saving progress")
                break
            raise

        searched += 1
        if url:
            row["youtube_url"] = url
            found += 1

        completed_indices.add(idx)
        label = song_name[:30] if row_type != "album" else album_name[:30]
        progress.update(extra=f"| found={found} api={enricher.api_calls}/{args.limit} {label}")

        if searched % 25 == 0:
            save_checkpoint({
                "completed_indices": sorted(completed_indices),
                "searched": searched,
                "found": found,
                "api_calls": enricher.api_calls,
            })
    progress.finish(extra=f"| found={found} api_calls={enricher.api_calls}")

    save_rows(rows, fieldnames)
    save_checkpoint({
        "completed_indices": sorted(completed_indices),
        "searched": searched,
        "found": found,
        "api_calls": enricher.api_calls,
    })

    print(f"searched={searched}")
    print(f"found={found}")
    print(f"api_calls={enricher.api_calls}")
    print(f"remaining={len(queue) - len(completed_indices)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
