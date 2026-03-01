#!/usr/bin/env python3
"""
Setlist.fm API exploration spike for P-Funk live recordings.

This is a standalone exploratory script -- NOT part of the daily pipeline.
It evaluates whether Setlist.fm data is reliable enough for matching
live album tracklists to actual concert performances.

Requires: SETLISTFM_API_KEY environment variable (free key from setlist.fm).
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import normalize_for_matching  # noqa: E402

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DISCOGRAPHY_PATH = ROOT / "data" / "discography.csv"
OUTPUT_DIR = ROOT / "reports" / "setlistfm_spike"

API_BASE = "https://api.setlist.fm/rest/1.0"
RATE_LIMIT_SECONDS = 0.6
USER_AGENT = "PFunkArchive/1.0 (https://github.com/pfunk-archive)"

CORE_MBIDS = {
    "George Clinton": "84683370-5eae-418b-acd8-883ac028a8a0",
    "Parliament": "d1947987-9614-49ae-bd36-8000e6b6f7d0",
    "Funkadelic": "cf042013-3edd-46c4-9b0e-a62faac98d0b",
    "Parliament-Funkadelic": None,
}


def get_api_key() -> str | None:
    return os.environ.get("SETLISTFM_API_KEY")


def fetch_setlists(
    session: requests.Session,
    artist_mbid: str,
    api_key: str,
    *,
    page: int = 1,
) -> dict | None:
    url = f"{API_BASE}/artist/{artist_mbid}/setlists?p={page}"
    time.sleep(RATE_LIMIT_SECONDS)
    try:
        resp = session.get(
            url,
            headers={
                "Accept": "application/json",
                "x-api-key": api_key,
            },
            timeout=30,
        )
        if resp.status_code in (404, 403):
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.warning("Request failed: %s", exc)
        return None


def extract_songs(setlist: dict) -> list[str]:
    """Extract song names from a setlist."""
    songs = []
    for s in setlist.get("sets", {}).get("set", []):
        for song in s.get("song", []):
            name = song.get("name", "").strip()
            if name:
                songs.append(name)
    return songs


def load_live_recordings() -> list[dict[str, str]]:
    """Load rows from discography that are live recordings."""
    if not DISCOGRAPHY_PATH.exists():
        return []
    with DISCOGRAPHY_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            r for r in reader
            if (r.get("version_type") or "").strip() == "live_recording"
            or "live" in (r.get("album_name") or "").lower()
        ]


def evaluate_coverage(
    setlist_songs: list[str],
    discography_songs: list[str],
) -> dict:
    """Evaluate how well setlist songs match discography songs."""
    norm_setlist = set(normalize_for_matching(s) for s in setlist_songs)
    norm_disco = set(normalize_for_matching(s) for s in discography_songs)

    matched = norm_setlist & norm_disco
    only_setlist = norm_setlist - norm_disco
    only_disco = norm_disco - norm_setlist

    return {
        "setlist_count": len(norm_setlist),
        "discography_count": len(norm_disco),
        "matched": len(matched),
        "match_rate": round(len(matched) / len(norm_disco), 2) if norm_disco else 0,
        "unmatched_setlist": sorted(only_setlist)[:10],
        "unmatched_disco": sorted(only_disco)[:10],
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    api_key = get_api_key()
    if not api_key:
        logger.error(
            "SETLISTFM_API_KEY not set. Get a free key at https://www.setlist.fm/settings/api"
        )
        print("\nThis script requires a Setlist.fm API key.")
        print("Set the SETLISTFM_API_KEY environment variable and try again.")
        print("The key is free -- sign up at https://www.setlist.fm/settings/api")
        return 1

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    live_rows = load_live_recordings()
    live_songs = [r.get("song_name", "") for r in live_rows if r.get("song_name")]

    logger.info("Found %d live recording rows in discography", len(live_rows))
    logger.info("Found %d unique live song titles", len(set(live_songs)))

    results: list[dict] = []
    total_setlists = 0

    for artist_name, mbid in CORE_MBIDS.items():
        if not mbid:
            continue
        logger.info("Fetching setlists for %s...", artist_name)

        data = fetch_setlists(session, mbid, api_key)
        if not data:
            logger.info("  No setlists found for %s", artist_name)
            continue

        setlists = data.get("setlist", [])
        total_available = data.get("total", 0)
        logger.info("  %s: %d setlists available (showing first page)", artist_name, total_available)
        total_setlists += total_available

        all_songs: list[str] = []
        for sl in setlists:
            songs = extract_songs(sl)
            all_songs.extend(songs)

        coverage = evaluate_coverage(all_songs, live_songs)
        results.append({
            "artist": artist_name,
            "setlists_available": total_available,
            "setlists_sampled": len(setlists),
            "unique_songs_in_setlists": len(set(normalize_for_matching(s) for s in all_songs)),
            **coverage,
        })

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / "spike_results.json"
    report_path.write_text(json.dumps({
        "summary": {
            "total_setlists_available": total_setlists,
            "live_discography_rows": len(live_rows),
            "artists_checked": len([r for r in results]),
        },
        "per_artist": results,
        "recommendation": (
            "Review the match_rate per artist. If > 0.5, Setlist.fm is likely useful "
            "for cross-validating live album tracklists. If < 0.3, the data may be too "
            "sparse or inconsistent for automated matching."
        ),
    }, indent=2), encoding="utf-8")

    print(f"\nSpike Results:")
    print(f"  Total setlists available: {total_setlists}")
    print(f"  Live discography songs: {len(set(live_songs))}")
    for r in results:
        print(f"  {r['artist']}: {r['setlists_available']} setlists, "
              f"match rate: {r['match_rate']:.0%}")
    print(f"\nFull report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
