#!/usr/bin/env python3
"""Enrich personnel credits from Discogs via MCP or direct API, with local cache."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import ProgressTracker  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
RELEASES_PATH = ROOT / "data" / "catalog_releases.csv"
CACHE_DIR = ROOT / "data" / ".discogs_cache"
PERSONNEL_OUT = ROOT / "data" / "catalog_personnel.csv"

CACHE_DIR.mkdir(parents=True, exist_ok=True)

PERSONNEL_FIELDS = [
    "release_id",
    "track_position",
    "person_name",
    "person_name_variant",
    "role",
    "discogs_artist_id",
    "discogs_release_id",
    "source",
]

DISCOGS_API_BASE = "https://api.discogs.com"
USER_AGENT = "PFunkArchive/1.0 +https://github.com/pfunk-archive"


def cache_key(namespace: str, identifier: str) -> str:
    raw = f"{namespace}:{identifier.strip().lower()}"
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


def discogs_get(path: str, token: str) -> dict[str, Any]:
    url = f"{DISCOGS_API_BASE}{path}"
    headers = {
        "User-Agent": USER_AGENT,
        "Authorization": f"Discogs token={token}",
    }
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 429:
            time.sleep(60)
            try:
                with urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except HTTPError:
                return {}
        if exc.code in (404, 403):
            return {}
        raise


def search_release(artist: str, title: str, token: str) -> dict[str, Any] | None:
    key = cache_key("search", f"{artist}|{title}")
    cached = cache_load(key)
    if cached is not None:
        return cached if cached.get("results") else None

    query = quote(f"{artist} {title}")
    path = f"/database/search?q={query}&type=master&per_page=5"
    try:
        result = discogs_get(path, token)
    except Exception:
        result = {"results": []}

    cache_save(key, result)
    time.sleep(1.1)

    if not result.get("results"):
        path = f"/database/search?q={query}&type=release&per_page=5"
        try:
            result = discogs_get(path, token)
        except Exception:
            result = {"results": []}
        cache_save(key, result)
        time.sleep(1.1)

    return result if result.get("results") else None


def get_master_release(master_id: int, token: str) -> dict[str, Any]:
    key = cache_key("master", str(master_id))
    cached = cache_load(key)
    if cached is not None:
        return cached

    result = discogs_get(f"/masters/{master_id}", token)
    cache_save(key, result)
    time.sleep(1.1)
    return result


def get_release(release_id: int, token: str) -> dict[str, Any]:
    key = cache_key("release", str(release_id))
    cached = cache_load(key)
    if cached is not None:
        return cached

    result = discogs_get(f"/releases/{release_id}", token)
    cache_save(key, result)
    time.sleep(1.1)
    return result


def norm(value: str) -> str:
    text = (value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_personnel(
    release_data: dict[str, Any],
    catalog_release_id: str,
    discogs_id: int,
) -> list[dict[str, str]]:
    personnel: list[dict[str, str]] = []

    for ea in release_data.get("extraartists", []):
        person_name = ea.get("name", "").strip()
        anv = ea.get("anv", "").strip()
        role = ea.get("role", "").strip()
        artist_id = ea.get("id", "")
        if not person_name or not role:
            continue
        personnel.append({
            "release_id": catalog_release_id,
            "track_position": "",
            "person_name": person_name,
            "person_name_variant": anv,
            "role": role,
            "discogs_artist_id": str(artist_id),
            "discogs_release_id": str(discogs_id),
            "source": "discogs",
        })

    for track in release_data.get("tracklist", []):
        pos = track.get("position", "").strip()
        for ea in track.get("extraartists", []):
            person_name = ea.get("name", "").strip()
            anv = ea.get("anv", "").strip()
            role = ea.get("role", "").strip()
            artist_id = ea.get("id", "")
            if not person_name or not role:
                continue
            personnel.append({
                "release_id": catalog_release_id,
                "track_position": pos,
                "person_name": person_name,
                "person_name_variant": anv,
                "role": role,
                "discogs_artist_id": str(artist_id),
                "discogs_release_id": str(discogs_id),
                "source": "discogs",
            })

        for artist in track.get("artists", []):
            person_name = artist.get("name", "").strip()
            anv = artist.get("anv", "").strip()
            artist_id = artist.get("id", "")
            if person_name:
                personnel.append({
                    "release_id": catalog_release_id,
                    "track_position": pos,
                    "person_name": person_name,
                    "person_name_variant": anv,
                    "role": "Performer",
                    "discogs_artist_id": str(artist_id),
                    "discogs_release_id": str(discogs_id),
                    "source": "discogs",
                })

    return personnel


def load_unique_releases() -> list[dict[str, str]]:
    if RELEASES_PATH.exists():
        with RELEASES_PATH.open("r", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    with CSV_PATH.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    seen: dict[str, dict[str, str]] = {}
    for row in rows:
        if row.get("row_type") != "album":
            continue
        key = f"{row['artist']}|{row['album_name']}|{row.get('release_date', '')}"
        if key not in seen:
            seen[key] = {
                "release_id": key,
                "artist_name": row["artist"],
                "album_name": row["album_name"],
                "release_date": row.get("release_date", ""),
            }
    return list(seen.values())


def main() -> int:
    token = os.getenv("DISCOGS_TOKEN", "").strip()
    if not token:
        print("Missing DISCOGS_TOKEN environment variable")
        return 1

    releases = load_unique_releases()
    all_personnel: list[dict[str, str]] = []
    searched = 0
    found = 0

    progress = ProgressTracker(total=len(releases), noun="releases")
    for rel in releases:
        artist = rel.get("artist_name", "")
        album = rel.get("album_name", "")
        release_id = rel.get("release_id", "")

        if not artist or not album:
            progress.update(extra="| skipped (missing data)")
            continue

        result = search_release(artist, album, token)
        searched += 1

        if not result or not result.get("results"):
            progress.update(extra=f"| {artist} - {album[:25]}: no match")
            continue

        first = result["results"][0]
        discogs_type = first.get("type", "")
        discogs_id = first.get("id", 0)

        if not discogs_id:
            progress.update(extra=f"| {artist} - {album[:25]}: no ID")
            continue

        if discogs_type == "master":
            release_data = get_master_release(discogs_id, token)
        else:
            release_data = get_release(discogs_id, token)

        personnel = extract_personnel(release_data, release_id, discogs_id)
        if personnel:
            all_personnel.extend(personnel)
            found += 1

        progress.update(extra=f"| {artist[:20]} - {album[:20]}: {len(personnel)} credits")

    progress.finish(extra=f"| {len(all_personnel)} personnel records")

    PERSONNEL_OUT.parent.mkdir(parents=True, exist_ok=True)
    with PERSONNEL_OUT.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PERSONNEL_FIELDS)
        writer.writeheader()
        writer.writerows(all_personnel)

    print(f"releases_searched={searched}")
    print(f"releases_with_credits={found}")
    print(f"total_personnel_records={len(all_personnel)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
