#!/usr/bin/env python3
"""
Scrape P-Funk Forums (pfunkforums.com) via the Discourse JSON API.

Fetches topics from Records/Books and Live Shows categories, extracts
title and tags as discovery hints, and writes to discovery_raw CSV.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from pathlib import Path

import sys

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import ProgressTracker  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent.parent
BASE_URL = "https://pfunkforums.com"
CACHE_DIR = ROOT / "data" / ".discovery_cache" / "pfunkforums.com"
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "pfunk_forums.csv"
CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
RATE_LIMIT_SECONDS = 2

CATEGORY_ENDPOINTS = [
    "/c/records-books/5.json",
    "/c/live-shows/6.json",
]


def url_to_cache_key(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def cache_get(url: str, force: bool) -> dict | None:
    key = url_to_cache_key(url)
    data_path = CACHE_DIR / key
    meta_path = CACHE_DIR / f"{key}.meta.json"
    if not force and data_path.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            fetched_at = meta.get("fetched_at")
            if fetched_at and (time.time() - fetched_at) < CACHE_TTL_SECONDS:
                return json.loads(data_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return None


def cache_put(url: str, data: dict) -> None:
    key = url_to_cache_key(url)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / key).write_text(json.dumps(data), encoding="utf-8")
    (CACHE_DIR / f"{key}.meta.json").write_text(
        json.dumps({"url": url, "fetched_at": time.time()}),
        encoding="utf-8",
    )


def fetch_url(url: str, force: bool, force_page_url: str | None) -> dict:
    full_url = BASE_URL + url if url.startswith("/") else url
    bypass_cache = force or (force_page_url == full_url)
    cached = cache_get(full_url, force=bypass_cache)
    if cached is not None:
        return cached
    time.sleep(RATE_LIMIT_SECONDS)
    resp = requests.get(full_url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    cache_put(full_url, data)
    return data


def tags_to_artist(tags: list[str]) -> str:
    if not tags:
        return ""
    non_meta = [
        t
        for t in tags
        if t.lower()
        not in ("book", "7-inch", "12-inch", "magazine", "comics", "bootleg", "stream")
    ]
    if not non_meta:
        return ""
    return ", ".join(t.replace("-", " ").title() for t in non_meta)


def topic_to_row(topic: dict) -> dict:
    topic_id = topic.get("id")
    slug = topic.get("slug", "")
    title = topic.get("title", "")
    tags = topic.get("tags") or []
    source_url = f"{BASE_URL}/t/{slug}/{topic_id}" if topic_id and slug else ""
    raw_extra = json.dumps(
        {
            "tags": tags,
            "views": topic.get("views"),
            "reply_count": topic.get("reply_count"),
            "created_at": topic.get("created_at"),
        }
    )
    return {
        "artist": tags_to_artist(tags),
        "album_name": title,
        "song_name": "",
        "release_date": "",
        "label": "",
        "row_type": "album",
        "discovery_source": "pfunk_forums",
        "source_url": source_url,
        "source_confidence": "low",
        "raw_extra": raw_extra,
    }


def collect_all_topics(force: bool, force_page_url: str | None) -> list[dict]:
    rows = []
    seen_ids: set[int] = set()
    progress = ProgressTracker(total=len(CATEGORY_ENDPOINTS), noun="categories", every=1)
    for cat_idx, endpoint in enumerate(CATEGORY_ENDPOINTS):
        page = 0
        while True:
            url = f"{endpoint}?page={page}" if page > 0 else endpoint
            data = fetch_url(url, force=force, force_page_url=force_page_url)
            topic_list = data.get("topic_list") or {}
            topics = topic_list.get("topics") or []
            if not topics:
                break
            for t in topics:
                tid = t.get("id")
                if tid and tid not in seen_ids:
                    seen_ids.add(tid)
                    rows.append(topic_to_row(t))
            sys.stderr.write(f"\r  Category {cat_idx + 1}/{len(CATEGORY_ENDPOINTS)}"
                             f" page {page + 1}: {len(rows)} topics so far   ")
            sys.stderr.flush()
            page += 1
        progress.update()
    progress.finish(extra=f"| {len(rows)} topics")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape P-Funk Forums via Discourse JSON API")
    parser.add_argument("--force", action="store_true", help="Re-fetch all URLs, ignoring cache")
    parser.add_argument(
        "--force-page",
        metavar="URL",
        default=None,
        help="Re-fetch only this specific URL",
    )
    args = parser.parse_args()
    force_page_url = args.force_page
    if force_page_url and not force_page_url.startswith(("http://", "https://")):
        force_page_url = BASE_URL + (force_page_url if force_page_url.startswith("/") else "/" + force_page_url)

    rows = collect_all_topics(force=args.force, force_page_url=force_page_url)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    columns = [
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
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    with_artist = sum(1 for r in rows if r["artist"])
    print(f"Topics: {len(rows)}")
    print(f"With artist from tags: {with_artist}")
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
