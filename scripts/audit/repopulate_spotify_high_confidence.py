#!/usr/bin/env python3
"""Repopulate missing Spotify URLs using strict high-confidence matching."""

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
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def norm(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"\((feat\.|ft\.|featuring)\s+[^)]*\)", "", text, flags=re.IGNORECASE)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_query_text(value: str, max_len: int = 110) -> str:
    text = (value or "").strip()
    text = re.sub(r"\((feat\.|ft\.|featuring)\s+[^)]*\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\[[^]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_len:
        return text
    words = text.split()
    trimmed = []
    current = 0
    for word in words:
        if current + len(word) + (1 if trimmed else 0) > max_len:
            break
        trimmed.append(word)
        current += len(word) + (1 if trimmed else 0)
    return " ".join(trimmed).strip() or text[:max_len].strip()


def similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, norm(left), norm(right)).ratio()


def cache_key(namespace: str, query: str, item_type: str) -> str:
    raw = f"{namespace}:{item_type}:{query.strip().lower()}"
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
    (CACHE_DIR / f"{key}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def search_with_cache(
    spotify: spotipy.Spotify,
    *,
    query: str,
    item_type: str,
    retry_limit: int = 3,
    sleep_seconds: float = 0.2,
) -> dict[str, Any]:
    key = cache_key("strict_repopulate", query, item_type)
    cached = cache_load(key)
    if cached is not None:
        return cached
    delay = sleep_seconds
    for attempt in range(retry_limit + 1):
        try:
            response = spotify.search(q=query, type=item_type, limit=10)
            cache_save(key, response)
            time.sleep(sleep_seconds)
            return response
        except SpotifyException as exc:
            if exc.http_status == 400:
                cache_save(key, {})
                return {}
            if exc.http_status == 429 and attempt < retry_limit:
                time.sleep(delay)
                delay *= 2
                continue
            raise
        except Exception:
            if attempt < retry_limit:
                time.sleep(delay)
                delay *= 2
                continue
            raise
    return {}


def choose_best_album_candidate(
    candidates: list[dict[str, Any]],
    *,
    artist: str,
    album_name: str,
) -> tuple[dict[str, Any] | None, float, dict[str, float]]:
    best = None
    best_score = 0.0
    best_parts: dict[str, float] = {}
    for item in candidates:
        title_score = similarity(album_name, item.get("name", ""))
        artists = [a.get("name", "") for a in item.get("artists", []) if isinstance(a, dict)]
        artist_score = max((similarity(artist, candidate) for candidate in artists), default=0.0)
        score = (0.55 * title_score) + (0.45 * artist_score)
        if score > best_score:
            best_score = score
            best = item
            best_parts = {"title_score": title_score, "artist_score": artist_score, "album_score": 1.0}
    return best, best_score, best_parts


def choose_best_track_candidate(
    candidates: list[dict[str, Any]],
    *,
    artist: str,
    song_name: str,
    album_name: str,
) -> tuple[dict[str, Any] | None, float, dict[str, float]]:
    best = None
    best_score = 0.0
    best_parts: dict[str, float] = {}
    for item in candidates:
        title_score = similarity(song_name, item.get("name", ""))
        artists = [a.get("name", "") for a in item.get("artists", []) if isinstance(a, dict)]
        artist_score = max((similarity(artist, candidate) for candidate in artists), default=0.0)
        album_score = similarity(album_name, (item.get("album") or {}).get("name", "")) if album_name else 1.0
        score = (0.45 * title_score) + (0.45 * artist_score) + (0.10 * album_score)
        if score > best_score:
            best_score = score
            best = item
            best_parts = {
                "title_score": title_score,
                "artist_score": artist_score,
                "album_score": album_score,
            }
    return best, best_score, best_parts


def accepted_high_confidence(
    *,
    row_type: str,
    score: float,
    title_score: float,
    artist_score: float,
    album_score: float,
) -> bool:
    if row_type == "album":
        return score >= 0.90 and title_score >= 0.90 and artist_score >= 0.82
    return score >= 0.88 and title_score >= 0.86 and artist_score >= 0.80 and album_score >= 0.65


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repopulate Spotify URLs with strict confidence")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--report-md", type=Path, required=True)
    parser.add_argument("--checkpoint-every", type=int, default=100)
    return parser.parse_args()


def build_markdown(report_md: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Strict Spotify Repopulation Report",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- rows_considered: {payload['metrics']['rows_considered']}",
        f"- found_high_confidence: {payload['metrics']['found_high_confidence']}",
        f"- moved_to_needs_research: {payload['metrics']['moved_to_needs_research']}",
        "",
        "## Accepted Matches",
        "",
        "| row_number | row_type | artist | expected_title | spotify_title | score |",
        "|---|---|---|---|---|---|",
    ]
    for item in payload.get("accepted", [])[:250]:
        lines.append(
            f"| {item['row_number']} | {item['row_type']} | {item['artist']} | "
            f"{item['expected_title']} | {item['spotify_title']} | {item['score']:.3f} |"
        )
    if not payload.get("accepted"):
        lines.append("| - | - | - | - | - | - |")

    lines.extend(
        [
            "",
            "## Rejected (Needs Research)",
            "",
            "| row_number | row_type | artist | expected_title | best_score | reason |",
            "|---|---|---|---|---|---|",
        ]
    )
    for item in payload.get("rejected", [])[:250]:
        lines.append(
            f"| {item['row_number']} | {item['row_type']} | {item['artist']} | "
            f"{item['expected_title']} | {item['best_score']:.3f} | {item['reason']} |"
        )
    if not payload.get("rejected"):
        lines.append("| - | - | - | - | - | - |")
    lines.append("")
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text("\n".join(lines), encoding="utf-8")


def persist_state(
    rows: list[dict[str, str]],
    fieldnames: list[str],
    tracking: dict[str, Any],
) -> None:
    safe_write_csv(CSV_PATH, rows, fieldnames)
    tracking["last_updated"] = datetime.now(timezone.utc).isoformat()
    TRACKING_PATH.write_text(json.dumps(tracking, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    rows, fieldnames = validate_csv_input(CSV_PATH, DISCOGRAPHY_COLUMNS, min_rows=1)
    tracking = json.loads(TRACKING_PATH.read_text(encoding="utf-8"))
    entries = tracking.get("entries", [])
    entries_by_row = {int(entry.get("row_number", 0)): entry for entry in entries if entry.get("row_number")}

    spotify = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=require_env("SPOTIPY_CLIENT_ID"),
            client_secret=require_env("SPOTIPY_CLIENT_SECRET"),
        )
    )

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    considered = 0
    missing_count = sum(1 for r in rows if not (r.get("spotify_url") or "").strip())
    progress = ProgressTracker(total=missing_count, noun="rows missing Spotify URL")

    for idx, row in enumerate(rows, start=2):
        if (row.get("spotify_url") or "").strip():
            continue

        row_type = (row.get("row_type") or "").strip()
        artist = row.get("artist", "")
        song_name = row.get("song_name", "")
        album_name = row.get("album_name", "")
        expected_title = album_name if row_type == "album" else song_name
        query_song = clean_query_text(song_name, max_len=100)
        query_album = clean_query_text(album_name, max_len=100)
        query_artist = clean_query_text(artist, max_len=80)
        considered += 1

        best_item = None
        best_score = 0.0
        score_parts = {"title_score": 0.0, "artist_score": 0.0, "album_score": 0.0}
        if row_type == "album":
            queries = [
                f'album:"{query_album}" artist:"{query_artist}"',
                f'"{query_album}" "{query_artist}"',
            ]
            for query in queries:
                result = search_with_cache(spotify, query=query, item_type="album")
                items = (result.get("albums") or {}).get("items", [])
                candidate, score, parts = choose_best_album_candidate(items, artist=artist, album_name=album_name)
                if score > best_score:
                    best_item = candidate
                    best_score = score
                    score_parts = parts
        else:
            queries = [
                f'track:"{query_song}" artist:"{query_artist}" album:"{query_album}"',
                f'track:"{query_song}" artist:"{query_artist}"',
                f'"{query_song}" "{query_artist}"',
            ]
            for query in queries:
                result = search_with_cache(spotify, query=query, item_type="track")
                items = (result.get("tracks") or {}).get("items", [])
                candidate, score, parts = choose_best_track_candidate(
                    items,
                    artist=artist,
                    song_name=song_name,
                    album_name=album_name,
                )
                if score > best_score:
                    best_item = candidate
                    best_score = score
                    score_parts = parts

        entry = entries_by_row.get(idx)
        if best_item and accepted_high_confidence(
            row_type=row_type,
            score=best_score,
            title_score=score_parts["title_score"],
            artist_score=score_parts["artist_score"],
            album_score=score_parts["album_score"],
        ):
            spotify_url = ((best_item.get("external_urls") or {}).get("spotify") or "").strip()
            if spotify_url:
                row["spotify_url"] = spotify_url
                accepted.append(
                    {
                        "row_number": idx,
                        "row_type": row_type,
                        "artist": artist,
                        "expected_title": expected_title,
                        "spotify_title": best_item.get("name", ""),
                        "spotify_url": spotify_url,
                        "score": round(best_score, 4),
                        "title_score": round(score_parts["title_score"], 4),
                        "artist_score": round(score_parts["artist_score"], 4),
                        "album_score": round(score_parts["album_score"], 4),
                    }
                )
                if entry:
                    entry["spotify_status"] = "found"
                    entry["confidence_level"] = "high" if best_score >= 0.93 else "medium"
                    entry["last_search_date"] = today
                    entry["search_attempts"] = int(entry.get("search_attempts", 0) or 0) + 1
                    history = entry.get("search_history", [])
                    if not isinstance(history, list):
                        history = []
                    history.append(
                        {
                            "date": today,
                            "round": "Strict Repopulation",
                            "method": "strict_high_confidence_search",
                            "result": "found",
                            "score": round(best_score, 4),
                        }
                    )
                    entry["search_history"] = history
                continue

        rejected.append(
            {
                "row_number": idx,
                "row_type": row_type,
                "artist": artist,
                "expected_title": expected_title,
                "best_score": round(best_score, 4),
                "title_score": round(score_parts["title_score"], 4),
                "artist_score": round(score_parts["artist_score"], 4),
                "album_score": round(score_parts["album_score"], 4),
                "reason": "no_high_confidence_match",
            }
        )
        if entry:
            entry["spotify_status"] = "needs_research"
            entry["confidence_level"] = "low"
            entry["last_search_date"] = today
            entry["search_attempts"] = int(entry.get("search_attempts", 0) or 0) + 1
            history = entry.get("search_history", [])
            if not isinstance(history, list):
                history = []
            history.append(
                {
                    "date": today,
                    "round": "Strict Repopulation",
                    "method": "strict_high_confidence_search",
                    "result": "not_found",
                    "score": round(best_score, 4),
                }
            )
            entry["search_history"] = history

        progress.update(extra=f"| accepted={len(accepted)} rejected={len(rejected)}")

        if args.checkpoint_every > 0 and considered % args.checkpoint_every == 0:
            persist_state(rows, fieldnames, tracking)

    progress.finish(extra=f"| accepted={len(accepted)} rejected={len(rejected)}")
    persist_state(rows, fieldnames, tracking)

    tracking["last_updated"] = datetime.now(timezone.utc).isoformat()
    if "statistics" not in tracking:
        tracking["statistics"] = {}
    tracking["statistics"]["total"] = len(entries)
    tracking["statistics"]["has_spotify_url"] = sum(1 for row in rows if (row.get("spotify_url") or "").strip())
    tracking["statistics"]["needs_research"] = sum(1 for entry in entries if entry.get("spotify_status") == "needs_research")
    tracking["statistics"]["not_searched"] = sum(1 for entry in entries if entry.get("spotify_status") == "not_searched")
    tracking["statistics"]["total_entries"] = len(entries)
    TRACKING_PATH.write_text(json.dumps(tracking, indent=2), encoding="utf-8")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "rows_considered": len(accepted) + len(rejected),
            "found_high_confidence": len(accepted),
            "moved_to_needs_research": len(rejected),
        },
        "accepted": accepted,
        "rejected": rejected,
    }
    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    build_markdown(args.report_md, payload)

    print(f"rows_considered={len(accepted) + len(rejected)}")
    print(f"found_high_confidence={len(accepted)}")
    print(f"moved_to_needs_research={len(rejected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
