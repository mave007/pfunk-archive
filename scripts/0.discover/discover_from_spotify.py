#!/usr/bin/env python3
"""
Discover albums from Spotify for artists already in the catalog.

Reads catalog artists, searches Spotify for each artist, retrieves their albums,
and outputs discovery rows to CSV with caching and rate limiting.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import time
from pathlib import Path

import sys

import spotipy
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import ProgressTracker  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
CATALOG_PATH = ROOT / "data" / "catalog_artists.csv"
CACHE_DIR = ROOT / "data" / ".spotify_cache"
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "spotify.csv"
RATE_LIMIT_SECONDS = 0.3

FIELDNAMES = [
    "artist",
    "album_name",
    "song_name",
    "release_date",
    "label",
    "row_type",
    "discovery_source",
    "source_url",
    "source_confidence",
    "raw_extra",
]


def normalize(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"\(feat\.[^)]+\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\(ft\.[^)]+\)", "", text, flags=re.IGNORECASE)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def cache_key(prefix: str, query: str) -> str:
    return hashlib.md5(f"{prefix}:{query.strip().lower()}".encode("utf-8")).hexdigest()


def cache_load(key: str) -> dict | None:
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def cache_save(key: str, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / f"{key}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def album_type_to_row_type(album_type: str) -> str:
    mapping = {"album": "album", "single": "single", "compilation": "compilation"}
    return mapping.get((album_type or "").lower(), "album")


def load_catalog_artists() -> set[str]:
    """Load artist names from catalog, falling back to discography if absent."""
    artists: set[str] = set()
    if CATALOG_PATH.exists():
        with CATALOG_PATH.open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name = row.get("artist_name_normalized", "").strip()
                if name:
                    artists.add(name)
    else:
        discography_path = ROOT / "data" / "discography.csv"
        if discography_path.exists():
            logging.warning(
                "catalog_artists.csv not found; falling back to discography.csv"
            )
            with discography_path.open("r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    name = (row.get("artist") or "").strip().lower()
                    if name:
                        artists.add(name)
        else:
            logging.error("Neither catalog_artists.csv nor discography.csv found")
    return artists


def search_artist(
    spotify: spotipy.Spotify,
    query: str,
    force: bool = False,
) -> dict | None:
    key = cache_key("artist_search", query)
    if not force:
        cached = cache_load(key)
        if cached is not None:
            return cached

    try:
        result = spotify.search(q=query, type="artist", limit=5)
        time.sleep(RATE_LIMIT_SECONDS)
    except SpotifyException as exc:
        logging.warning("Spotify search failed for %r: %s", query, exc)
        return None
    except Exception as exc:
        logging.warning("Spotify search failed for %r: %s", query, exc)
        return None

    cache_save(key, result)
    return result


def get_artist_albums(
    spotify: spotipy.Spotify,
    artist_id: str,
    force: bool = False,
) -> list[dict] | None:
    key = cache_key("artist_albums", artist_id)
    if not force:
        cached = cache_load(key)
        if cached is not None:
            return cached.get("items", [])

    try:
        all_items: list[dict] = []
        result = spotify.artist_albums(
            artist_id,
            limit=50,
            include_groups="album,single,compilation",
        )
        time.sleep(RATE_LIMIT_SECONDS)
        all_items.extend(result.get("items", []))

        while result.get("next"):
            result = spotify.next(result)
            time.sleep(RATE_LIMIT_SECONDS)
            all_items.extend(result.get("items", []))

        cache_save(key, {"items": all_items})
        return all_items
    except SpotifyException as exc:
        logging.warning("Spotify artist_albums failed for %s: %s", artist_id, exc)
        return None
    except Exception as exc:
        logging.warning("Spotify artist_albums failed for %s: %s", artist_id, exc)
        return None


def run(
    force: bool = False,
    force_artist: str | None = None,
) -> tuple[list[dict], int, int]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    client_id = os.getenv("SPOTIPY_CLIENT_ID", "").strip()
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("Set SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET")

    spotify = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret,
        )
    )

    catalog_artists = load_catalog_artists()
    if force_artist:
        artist_norm = normalize(force_artist)
        catalog_artists = {a for a in catalog_artists if normalize(a) == artist_norm}
        if not catalog_artists:
            catalog_artists = {force_artist}

    artists_searched = 0
    albums_found = 0
    seen: set[tuple[str, str]] = set()
    rows: list[dict] = []
    sorted_artists = sorted(catalog_artists)

    progress = ProgressTracker(total=len(sorted_artists), noun="artists")
    for artist_name_norm in sorted_artists:
        bypass_cache = force or (force_artist is not None)

        search_result = search_artist(spotify, artist_name_norm, force=bypass_cache)
        if not search_result:
            progress.update(extra=f"| {artist_name_norm}: no results")
            continue

        artists = search_result.get("artists", {}).get("items", [])
        if not artists:
            progress.update(extra=f"| {artist_name_norm}: no artists")
            continue

        artists_searched += 1
        best = artists[0]
        artist_id = best.get("id")
        spotify_artist_name = best.get("name", "")

        if not artist_id:
            progress.update(extra=f"| {artist_name_norm}: no ID")
            continue

        is_exact_match = normalize(spotify_artist_name) == normalize(artist_name_norm)
        confidence = "high" if is_exact_match else "medium"

        albums = get_artist_albums(spotify, artist_id, force=bypass_cache)
        if albums is None:
            progress.update(extra=f"| {artist_name_norm}: album fetch failed")
            continue

        artist_albums_count = 0
        for alb in albums:
            album_name = alb.get("name", "")
            if not album_name:
                continue

            release_date = alb.get("release_date", "")
            external = alb.get("external_urls", {}) or {}
            source_url = external.get("spotify", "")

            album_type = alb.get("album_type", "album")
            row_type = album_type_to_row_type(album_type)

            raw_extra = json.dumps({
                "spotify_id": alb.get("id"),
                "album_type": album_type,
                "total_tracks": alb.get("total_tracks"),
            })

            dedupe_key = (normalize(spotify_artist_name), normalize(album_name))
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            albums_found += 1
            artist_albums_count += 1
            rows.append({
                "artist": spotify_artist_name,
                "album_name": album_name,
                "song_name": "",
                "release_date": release_date,
                "label": "",
                "row_type": row_type,
                "discovery_source": "spotify",
                "source_url": source_url,
                "source_confidence": confidence,
                "raw_extra": raw_extra,
            })

        progress.update(extra=f"| {artist_name_norm}: {artist_albums_count} albums")
    progress.finish(extra=f"| {albums_found} total albums")

    return rows, artists_searched, albums_found


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(
        description="Discover albums from Spotify for catalog artists.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-crawl all artists, ignoring cache",
    )
    parser.add_argument(
        "--force-artist",
        metavar="NAME",
        help="Re-crawl only this artist (match by artist_name_normalized)",
    )
    args = parser.parse_args()

    rows, artists_searched, albums_found = run(
        force=args.force,
        force_artist=args.force_artist,
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"artists_searched={artists_searched}")
    print(f"albums_found={albums_found}")
    print(f"rows_written={len(rows)}")


if __name__ == "__main__":
    main()
