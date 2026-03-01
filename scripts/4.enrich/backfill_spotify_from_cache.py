#!/usr/bin/env python3
"""Backfill missing Spotify URLs using local Spotify cache only."""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import DISCOGRAPHY_COLUMNS, safe_write_csv, validate_csv_input  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
CACHE_DIR = ROOT / "data" / ".spotify_cache"


def normalize(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"\(feat\.[^)]+\)", "", text)
    text = re.sub(r"\(ft\.[^)]+\)", "", text)
    text = re.sub(r"^\[[^\]]+\]\s*", "", text)
    text = text.replace("&", "and")
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    text = re.sub(r"\bthe\b", "", text).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def row_key(row: dict[str, str]) -> tuple[str, str]:
    artist = normalize(row.get("artist", ""))
    if row.get("row_type") == "album":
        title = normalize(row.get("album_name", ""))
    else:
        title = normalize(row.get("song_name", ""))
    return artist, title


def extract_cache_maps() -> tuple[dict[tuple[str, str], str], dict[tuple[str, str], str]]:
    track_map: dict[tuple[str, str], str] = {}
    album_map: dict[tuple[str, str], str] = {}

    for cache_file in CACHE_DIR.glob("*.json"):
        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue

        tracks = payload.get("tracks", {})
        if isinstance(tracks, dict):
            items = tracks.get("items", [])
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    url = item.get("external_urls", {}).get("spotify", "")
                    if not url:
                        continue
                    title = normalize(item.get("name", ""))
                    artists = item.get("artists", [])
                    if not isinstance(artists, list):
                        continue
                    for artist_item in artists:
                        if not isinstance(artist_item, dict):
                            continue
                        artist_name = normalize(artist_item.get("name", ""))
                        if artist_name and title:
                            track_map[(artist_name, title)] = url

        albums = payload.get("albums", {})
        if isinstance(albums, dict):
            items = albums.get("items", [])
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    url = item.get("external_urls", {}).get("spotify", "")
                    if not url:
                        continue
                    album_title = normalize(item.get("name", ""))
                    artists = item.get("artists", [])
                    if not isinstance(artists, list):
                        continue
                    for artist_item in artists:
                        if not isinstance(artist_item, dict):
                            continue
                        artist_name = normalize(artist_item.get("name", ""))
                        if artist_name and album_title:
                            album_map[(artist_name, album_title)] = url

    return track_map, album_map


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Backfill Spotify URLs from cache")
    parser.add_argument("--sidecar", action="store_true", help="Write results to sidecar file instead of modifying discography.csv")
    args = parser.parse_args()

    track_map, album_map = extract_cache_maps()

    rows, fieldnames = validate_csv_input(CSV_PATH, DISCOGRAPHY_COLUMNS, min_rows=1)

    sidecar_rows: list[dict[str, str]] = []
    updated = 0
    for idx, row in enumerate(rows):
        if row.get("spotify_url", "").strip():
            continue
        artist_key, title_key = row_key(row)
        if not artist_key or not title_key:
            continue
        if row.get("row_type") == "album":
            url = album_map.get((artist_key, title_key))
        else:
            url = track_map.get((artist_key, title_key))
        if url:
            if args.sidecar:
                sidecar_rows.append({"row_index": str(idx), "field_name": "spotify_url", "value": url})
            else:
                row["spotify_url"] = url
            updated += 1

    if args.sidecar:
        sidecar_dir = ROOT / "data" / ".enrich_sidecars"
        sidecar_dir.mkdir(parents=True, exist_ok=True)
        safe_write_csv(
            sidecar_dir / "backfill_spotify.csv",
            sidecar_rows,
            ["row_index", "field_name", "value"],
            backup=False,
        )
    else:
        safe_write_csv(CSV_PATH, rows, fieldnames)

    print(f"cache_backfilled_spotify_urls={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
