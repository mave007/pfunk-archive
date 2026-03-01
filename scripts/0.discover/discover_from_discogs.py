#!/usr/bin/env python3
"""
Crawl the Discogs API to discover releases for P-Funk artists listed in the seeds file.

Reads artist IDs from data/discovery_seeds.csv, fetches releases per artist with
pagination, caches responses in data/.discogs_cache/, and outputs to
data/discovery_raw/discogs.csv.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
from pathlib import Path

import sys

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import ProgressTracker  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
SEEDS_PATH = ROOT / "data" / "discovery_seeds.csv"
CACHE_DIR = ROOT / "data" / ".discogs_cache"
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "discogs.csv"

API_BASE = "https://api.discogs.com"
USER_AGENT = "PFunkArchive/1.0 +https://github.com/pfunk-archive"
SLEEP_BETWEEN_REQUESTS = 1.1
SLEEP_ON_RATE_LIMIT = 60


def cache_path(url: str) -> Path:
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{key}.json"


def load_seeds(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def fetch_page(
    url: str,
    session: requests.Session,
    token: str,
    force: bool,
) -> dict | None:
    full_url = f"{url}&token={token}" if "?" in url else f"{url}?token={token}"
    cache_file = cache_path(full_url)

    if not force and cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    resp = session.get(full_url, timeout=30)

    if resp.status_code == 429:
        time.sleep(SLEEP_ON_RATE_LIMIT)
        resp = session.get(full_url, timeout=30)

    if resp.status_code in (404, 403):
        return None

    resp.raise_for_status()
    data = resp.json()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    return data


def releases_for_artist(
    artist_id: str,
    artist_name: str,
    session: requests.Session,
    token: str,
    force: bool,
) -> list[dict]:
    rows: list[dict] = []
    page = 1
    while True:
        url = f"{API_BASE}/artists/{artist_id}/releases?per_page=100&page={page}"
        data = fetch_page(url, session, token, force)

        if data is None:
            if page == 1:
                print(f"Warning: skipping artist {artist_name} ({artist_id}): HTTP 404 or 403")
            return rows

        releases = data.get("releases", [])
        pagination = data.get("pagination", {})
        pages = pagination.get("pages", 1)

        for rel in releases:
            raw_extra = {
                "discogs_id": rel.get("id"),
                "format": rel.get("format"),
                "type": rel.get("type"),
                "role": rel.get("role"),
            }
            release_type = (rel.get("type") or "").lower()
            row_type = "album" if release_type in ("master", "release") else "album"

            label_val = rel.get("label")
            if isinstance(label_val, list) and label_val:
                first = label_val[0]
                label_str = first.get("name", first) if isinstance(first, dict) else str(first)
            else:
                label_str = str(label_val) if label_val else ""

            rows.append(
                {
                    "artist": artist_name,
                    "album_name": rel.get("title", ""),
                    "song_name": "",
                    "release_date": str(rel.get("year", "")) if rel.get("year") else "",
                    "label": label_str,
                    "row_type": row_type,
                    "discovery_source": "discogs",
                    "source_url": rel.get("resource_url", ""),
                    "source_confidence": "high",
                    "raw_extra": json.dumps(raw_extra),
                }
            )

        if page >= pages:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return rows


def run(
    force: bool,
    force_artist: str | None,
) -> tuple[list[dict], int, int]:
    token = os.environ.get("DISCOGS_TOKEN")
    if not token:
        raise SystemExit("DISCOGS_TOKEN environment variable is required")

    seeds = load_seeds(SEEDS_PATH)
    if not seeds:
        return [], 0, 0

    if force_artist:
        matches = [
            s
            for s in seeds
            if str(s.get("discogs_artist_id", "")) == force_artist
            or (s.get("artist_name", "") or "").lower() == force_artist.lower()
        ]
        if not matches:
            print(f"No artist matching '{force_artist}', exiting.")
            return [], 0, 0
        seeds = matches

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    all_rows: list[dict] = []
    artists_crawled = 0

    progress = ProgressTracker(total=len(seeds), noun="artists")
    for seed in seeds:
        artist_id = seed.get("discogs_artist_id", "").strip()
        artist_name = seed.get("artist_name", "").strip()
        if not artist_id:
            progress.update(extra=f"| skipped (no ID)")
            continue

        artist_force = force or (force_artist is not None)
        try:
            rows = releases_for_artist(
                artist_id, artist_name, session, token, force=artist_force
            )
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (404, 403):
                print(f"Warning: skipping artist {artist_name} ({artist_id}): HTTP {e.response.status_code}")
                progress.update(extra=f"| {artist_name}: HTTP {e.response.status_code}")
                continue
            raise

        all_rows.extend(rows)
        if rows:
            artists_crawled += 1

        progress.update(extra=f"| {artist_name}: {len(rows)} releases")
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    progress.finish(extra=f"| {len(all_rows)} total releases")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
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

    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    return all_rows, artists_crawled, len(seeds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover P-Funk releases from Discogs API using discovery seeds."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-crawl all artists, ignoring cache",
    )
    parser.add_argument(
        "--force-artist",
        metavar="NAME_OR_ID",
        help="Re-crawl only a specific artist (match by name or discogs_artist_id)",
    )
    parser.add_argument(
        "--force-album",
        metavar="NAME",
        help="(Placeholder, not used in this script)",
    )
    args = parser.parse_args()

    rows, artists_crawled, artists_total = run(
        force=args.force,
        force_artist=args.force_artist,
    )

    print(f"Artists crawled: {artists_crawled} of {artists_total}")
    print(f"Total releases found: {len(rows)}")
    print(f"Rows written: {len(rows)} to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
