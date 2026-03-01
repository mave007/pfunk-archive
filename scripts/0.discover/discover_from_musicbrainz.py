#!/usr/bin/env python3
"""
Discover releases from MusicBrainz for P-Funk artists.

Reads artist MBIDs from data/discovery_seeds.csv (musicbrainz_artist_id column),
fetches release groups and recordings via the MusicBrainz JSON API, caches
responses, and outputs discovery rows to data/discovery_raw/musicbrainz.csv.

MusicBrainz API: no key needed, 1 request/second rate limit, requires User-Agent.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import DISCOVERY_SOURCE_COLUMNS, safe_write_csv  # noqa: E402

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
SEEDS_PATH = ROOT / "data" / "discovery_seeds.csv"
CACHE_DIR = ROOT / "data" / ".musicbrainz_cache"
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "musicbrainz.csv"

API_BASE = "https://musicbrainz.org/ws/2"
USER_AGENT = "PFunkArchive/1.0 ( https://github.com/pfunk-archive )"
RATE_LIMIT_SECONDS = 1.1


def cache_key(url: str) -> Path:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{h}.json"


def fetch_json(
    session: requests.Session,
    url: str,
    *,
    force: bool = False,
) -> dict | None:
    cached = cache_key(url)
    if not force and cached.exists():
        try:
            return json.loads(cached.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    time.sleep(RATE_LIMIT_SECONDS)
    try:
        resp = session.get(url, timeout=30)
    except requests.RequestException as exc:
        logger.warning("Request failed for %s: %s", url, exc)
        return None

    if resp.status_code == 503:
        logger.warning("Rate limited, sleeping 5s and retrying")
        time.sleep(5)
        resp = session.get(url, timeout=30)

    if resp.status_code in (404, 400):
        return None

    resp.raise_for_status()
    data = resp.json()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cached.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data


def release_group_type_to_row_type(rg_type: str) -> str:
    mapping = {
        "Album": "album",
        "Single": "single",
        "EP": "album",
        "Compilation": "album",
        "Live": "album",
        "Remix": "album",
    }
    return mapping.get(rg_type or "", "album")


def fetch_release_groups(
    session: requests.Session,
    mbid: str,
    artist_name: str,
    *,
    force: bool = False,
) -> list[dict[str, str]]:
    """Fetch all release groups for an artist MBID."""
    rows: list[dict[str, str]] = []
    offset = 0
    limit = 100

    while True:
        url = (
            f"{API_BASE}/release-group?artist={mbid}"
            f"&limit={limit}&offset={offset}&fmt=json"
        )
        data = fetch_json(session, url, force=force)
        if not data:
            break

        for rg in data.get("release-groups", []):
            title = rg.get("title", "").strip()
            if not title:
                continue

            rg_type = rg.get("primary-type", "Album")
            first_release = rg.get("first-release-date", "")

            rows.append({
                "artist": artist_name,
                "album_name": title,
                "song_name": "",
                "release_date": first_release or "",
                "label": "",
                "row_type": release_group_type_to_row_type(rg_type),
                "discovery_source": "musicbrainz",
                "source_url": f"https://musicbrainz.org/release-group/{rg.get('id', '')}",
                "source_confidence": "high",
                "raw_extra": json.dumps({
                    "mbid": rg.get("id", ""),
                    "primary_type": rg_type,
                    "secondary_types": rg.get("secondary-types", []),
                }),
            })

        total = data.get("release-group-count", 0)
        offset += limit
        if offset >= total:
            break

    return rows


def fetch_recordings(
    session: requests.Session,
    mbid: str,
    artist_name: str,
    *,
    force: bool = False,
    limit_pages: int = 5,
) -> list[dict[str, str]]:
    """Fetch recordings for an artist to get track-level data with durations."""
    rows: list[dict[str, str]] = []
    offset = 0
    limit = 100
    pages = 0

    while pages < limit_pages:
        url = (
            f"{API_BASE}/recording?artist={mbid}"
            f"&limit={limit}&offset={offset}&fmt=json"
        )
        data = fetch_json(session, url, force=force)
        if not data:
            break

        for rec in data.get("recordings", []):
            title = rec.get("title", "").strip()
            if not title:
                continue

            duration_ms = rec.get("length")
            duration_sec = str(round(duration_ms / 1000)) if duration_ms else ""

            rows.append({
                "artist": artist_name,
                "album_name": "",
                "song_name": title,
                "release_date": "",
                "label": "",
                "row_type": "track",
                "discovery_source": "musicbrainz",
                "source_url": f"https://musicbrainz.org/recording/{rec.get('id', '')}",
                "source_confidence": "high",
                "raw_extra": json.dumps({
                    "mbid": rec.get("id", ""),
                    "duration_seconds": duration_sec,
                    "isrcs": rec.get("isrcs", []),
                }),
            })

        total = data.get("recording-count", 0)
        offset += limit
        pages += 1
        if offset >= total:
            break

    return rows


def load_seeds() -> list[dict[str, str]]:
    if not SEEDS_PATH.exists():
        logger.error("Seeds file not found: %s", SEEDS_PATH)
        return []
    with SEEDS_PATH.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Discover releases from MusicBrainz")
    parser.add_argument("--force", action="store_true", help="Re-fetch all (ignore cache)")
    parser.add_argument(
        "--force-artist", type=str, default=None,
        help="Re-fetch only this artist (name or MBID)"
    )
    parser.add_argument(
        "--recordings", action="store_true",
        help="Also fetch recordings (track-level data with durations)"
    )
    args = parser.parse_args()

    seeds = load_seeds()
    if not seeds:
        return 1

    has_mbid_column = any("musicbrainz_artist_id" in s for s in seeds)
    if not has_mbid_column:
        logger.error(
            "discovery_seeds.csv has no musicbrainz_artist_id column. "
            "Add MBIDs for artists to enable MusicBrainz discovery."
        )
        return 1

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.headers["Accept"] = "application/json"

    all_rows: list[dict[str, str]] = []
    artists_processed = 0

    for seed in seeds:
        mbid = (seed.get("musicbrainz_artist_id") or "").strip()
        if not mbid:
            continue

        artist_name = seed.get("artist_name", "").strip()
        force_this = args.force or (
            args.force_artist is not None
            and args.force_artist.lower() in (mbid.lower(), artist_name.lower())
        )

        logger.info("Fetching release groups for %s (%s)", artist_name, mbid)
        rg_rows = fetch_release_groups(session, mbid, artist_name, force=force_this)
        all_rows.extend(rg_rows)

        if args.recordings:
            logger.info("Fetching recordings for %s (%s)", artist_name, mbid)
            rec_rows = fetch_recordings(session, mbid, artist_name, force=force_this)
            all_rows.extend(rec_rows)

        artists_processed += 1

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    safe_write_csv(OUTPUT_PATH, all_rows, DISCOVERY_SOURCE_COLUMNS, backup=False)

    logger.info(
        "Done: %d artists processed, %d rows written to %s",
        artists_processed,
        len(all_rows),
        OUTPUT_PATH.name,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
