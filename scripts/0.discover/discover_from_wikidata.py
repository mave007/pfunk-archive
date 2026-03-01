#!/usr/bin/env python3
"""
Discover releases and cross-validate data from Wikidata via SPARQL.

Queries the Wikidata SPARQL endpoint for albums/singles/compilations
performed by core P-Funk artists, harvests external IDs (AllMusic,
Discogs, MusicBrainz), and outputs discovery rows.

No auth needed; generous rate limits.
"""

from __future__ import annotations

import csv
import json
import logging
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import DISCOVERY_SOURCE_COLUMNS, safe_write_csv  # noqa: E402

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
SEEDS_PATH = ROOT / "data" / "discovery_seeds.csv"
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "wikidata.csv"
CACHE_DIR = ROOT / "data" / ".wikidata_cache"

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "PFunkArchive/1.0 (https://github.com/pfunk-archive) Python/requests"
RATE_LIMIT_SECONDS = 2.0

SPARQL_TEMPLATE = """
SELECT ?item ?itemLabel ?date ?itemDescription
       ?discogsId ?musicbrainzId ?allMusicId ?spotifyId
WHERE {{
  ?item wdt:P175 wd:{artist_qid} .
  ?item wdt:P31/wdt:P279* wd:Q482994 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  OPTIONAL {{ ?item wdt:P1954 ?discogsId . }}
  OPTIONAL {{ ?item wdt:P436 ?musicbrainzId . }}
  OPTIONAL {{ ?item wdt:P1994 ?allMusicId . }}
  OPTIONAL {{ ?item wdt:P2207 ?spotifyId . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
ORDER BY ?date
"""

CORE_ARTIST_QIDS = {
    "George Clinton": "Q192640",
    "Parliament": "Q1192367",
    "Funkadelic": "Q722230",
    "Bootsy Collins": "Q467837",
    "Bootsy's Rubber Band": "Q4943805",
    "Brides of Funkenstein": "Q4966618",
    "Parlet": "Q7138834",
    "Eddie Hazel": "Q1282612",
    "Bernie Worrell": "Q552405",
    "Ruth Copeland": "Q7382864",
}


def run_sparql(session: requests.Session, query: str, *, force: bool = False) -> list[dict]:
    """Execute SPARQL query and return bindings list, with caching."""
    import hashlib

    cache_key = hashlib.md5(query.encode("utf-8")).hexdigest()
    cache_file = CACHE_DIR / f"{cache_key}.json"

    if not force and cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    time.sleep(RATE_LIMIT_SECONDS)
    try:
        resp = session.get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "json"},
            timeout=60,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("SPARQL query failed: %s", exc)
        return []

    data = resp.json()
    bindings = data.get("results", {}).get("bindings", [])

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(bindings, indent=2), encoding="utf-8")

    return bindings


def binding_value(binding: dict, key: str) -> str:
    return (binding.get(key, {}).get("value") or "").strip()


def bindings_to_rows(bindings: list[dict], artist_name: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for b in bindings:
        title = binding_value(b, "itemLabel")
        if not title:
            continue

        qid = binding_value(b, "item").rsplit("/", 1)[-1] if binding_value(b, "item") else ""
        date_raw = binding_value(b, "date")
        release_date = date_raw[:10] if date_raw else ""

        extra = {}
        for key, label in [
            ("discogsId", "discogs_release_id"),
            ("musicbrainzId", "musicbrainz_release_id"),
            ("allMusicId", "allmusic_id"),
            ("spotifyId", "spotify_album_id"),
        ]:
            val = binding_value(b, key)
            if val:
                extra[label] = val
        extra["wikidata_qid"] = qid

        rows.append({
            "artist": artist_name,
            "album_name": title,
            "song_name": "",
            "release_date": release_date,
            "label": "",
            "row_type": "album",
            "discovery_source": "wikidata",
            "source_url": f"https://www.wikidata.org/wiki/{qid}" if qid else "",
            "source_confidence": "medium",
            "raw_extra": json.dumps(extra) if extra else "",
        })

    return rows


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.headers["Accept"] = "application/sparql-results+json"

    all_rows: list[dict[str, str]] = []
    artists_queried = 0

    for artist_name, qid in CORE_ARTIST_QIDS.items():
        logger.info("Querying Wikidata for %s (Q%s)", artist_name, qid.lstrip("Q"))
        query = SPARQL_TEMPLATE.format(artist_qid=qid)
        bindings = run_sparql(session, query)
        rows = bindings_to_rows(bindings, artist_name)
        all_rows.extend(rows)
        artists_queried += 1
        logger.info("  Found %d items for %s", len(rows), artist_name)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    safe_write_csv(OUTPUT_PATH, all_rows, DISCOVERY_SOURCE_COLUMNS, backup=False)

    logger.info(
        "Done: %d artists queried, %d rows written to %s",
        artists_queried,
        len(all_rows),
        OUTPUT_PATH.name,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
