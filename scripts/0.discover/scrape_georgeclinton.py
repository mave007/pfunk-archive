#!/usr/bin/env python3
"""
Scrape https://georgeclinton.com/music/ to discover P-Funk releases.

Parses the main discography page for album titles and links, follows detail pages
at /audio/<slug>/ for track listings, and outputs discovery rows to CSV.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

import sys

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import ProgressTracker  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
BASE_URL = "https://georgeclinton.com"
MUSIC_URL = f"{BASE_URL}/music/"
CACHE_DIR = ROOT / "data" / ".discovery_cache" / "georgeclinton.com"
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "georgeclinton.csv"
CACHE_TTL_DAYS = 7
RATE_LIMIT_SECONDS = 2
USER_AGENT = "Mozilla/5.0 (compatible; pfunk-archive/1.0; +https://github.com/pfunk-archive)"


def cache_path(url: str) -> tuple[Path, Path]:
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / key, CACHE_DIR / f"{key}.meta.json"


def is_cache_valid(meta_path: Path) -> bool:
    if not meta_path.exists():
        return False
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        ts = meta.get("timestamp")
        if not ts:
            return False
        cached_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - cached_time) < timedelta(days=CACHE_TTL_DAYS)
    except (json.JSONDecodeError, (KeyError, ValueError)):
        return False


def load_from_cache(html_path: Path, meta_path: Path) -> tuple[str | None, int | None]:
    if not html_path.exists():
        return None, None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return html_path.read_text(encoding="utf-8"), meta.get("status_code")
    except (json.JSONDecodeError, OSError):
        return None, None


def save_to_cache(html_path: Path, meta_path: Path, url: str, html: str, status_code: int) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html, encoding="utf-8")
    meta_path.write_text(
        json.dumps(
            {
                "url": url,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status_code": status_code,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def fetch(
    url: str,
    session: requests.Session,
    force: bool = False,
    force_page: str | None = None,
) -> tuple[str, int]:
    html_path, meta_path = cache_path(url)
    if not force and (force_page is None or url != force_page):
        if is_cache_valid(meta_path):
            content, status = load_from_cache(html_path, meta_path)
            if content is not None and status is not None:
                return content, status

    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    save_to_cache(html_path, meta_path, url, resp.text, resp.status_code)
    return resp.text, resp.status_code


def parse_artist_from_title(title: str) -> str:
    format_suffixes = ("single", "live", "ep")
    for sep in (" – ", " - "):
        if sep in title:
            parts = title.split(sep, 1)
            if len(parts) == 2 and parts[1].strip().lower() not in format_suffixes:
                if parts[0].strip():
                    return parts[0].strip()
    return "George Clinton"


def extract_album_entries(soup: BeautifulSoup) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    seen_urls: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        parsed = urlparse(urljoin(BASE_URL, href))
        path = parsed.path.rstrip("/")
        if not path.startswith("/audio/") or path == "/audio":
            continue
        detail_url = urljoin(BASE_URL, href)
        if detail_url in seen_urls:
            continue

        title = None
        parent = a.parent
        for _ in range(5):
            if parent is None:
                break
            for tag in ("h2", "h3", "h4", "h5"):
                header = parent.find(tag)
                if header and header.get_text(strip=True):
                    title = header.get_text(strip=True)
                    break
            if title:
                break
            prev = parent.find_previous_sibling()
            if prev:
                header = prev.find(["h2", "h3", "h4", "h5"])
                if header and header.get_text(strip=True):
                    title = header.get_text(strip=True)
                    break
            parent = parent.parent

        if not title and a.string:
            title = a.get_text(strip=True)
        if not title:
            title = path.replace("/audio/", "").replace("-", " ").title()

        title = title.replace("Read more", "").strip()
        if not title or len(title) < 2:
            continue

        seen_urls.add(detail_url)
        entries.append((title, detail_url))

    return entries


def extract_tracks(html: str, detail_url: str) -> list[tuple[str, int | None]]:
    soup = BeautifulSoup(html, "html.parser")
    tracks: list[tuple[str, int | None]] = []

    for elem in soup.find_all(["p", "div", "li"]):
        text = elem.get_text(separator=" ", strip=True)
        if not text or len(text) < 3:
            continue

        pattern = r'(\d{1,2})\.\s*["\u201c\u201d]?([^"\d]+?)["\u201c\u201d]?(?=\s*\d{1,2}\.\s|$)'
        matches = re.findall(pattern, text)
        for num_str, name in matches:
            name = name.strip().strip('"\u201c\u201d')
            if len(name) < 2 or name.lower() in ("read more", "related posts"):
                continue
            num = int(num_str)
            tracks.append((name, num))

    if not tracks:
        for elem in soup.find_all("ol"):
            for i, li in enumerate(elem.find_all("li", recursive=False), 1):
                name = li.get_text(strip=True)
                if name and len(name) > 1:
                    tracks.append((name, i))

    return tracks


def run(force: bool, force_page: str | None) -> list[dict[str, str]]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    rows: list[dict[str, str]] = []

    html, _ = fetch(MUSIC_URL, session, force=force, force_page=force_page)
    time.sleep(RATE_LIMIT_SECONDS)

    soup = BeautifulSoup(html, "html.parser")
    entries = extract_album_entries(soup)

    progress = ProgressTracker(total=len(entries), noun="albums")
    for album_title, detail_url in entries:
        artist = parse_artist_from_title(album_title)
        album_name = album_title
        for sep in (" – ", " - "):
            if sep in album_title:
                parts = album_title.split(sep, 1)
                if parts[1].strip().lower() not in ("single", "live", "ep"):
                    album_name = parts[-1].strip()
                else:
                    album_name = parts[0].strip()
                break
        if not album_name:
            album_name = album_title

        rows.append(
            {
                "artist": artist,
                "album_name": album_name,
                "song_name": "",
                "release_date": "",
                "label": "",
                "row_type": "album",
                "discovery_source": "georgeclinton",
                "source_url": detail_url,
                "source_confidence": "high",
                "raw_extra": json.dumps({"detail_url": detail_url}),
            }
        )

        detail_html, _ = fetch(detail_url, session, force=force, force_page=force_page)
        time.sleep(RATE_LIMIT_SECONDS)

        seen_tracks: set[tuple[str, int | None]] = set()
        for song_name, track_num in extract_tracks(detail_html, detail_url):
            key = (song_name, track_num)
            if key in seen_tracks:
                continue
            seen_tracks.add(key)
            rows.append(
                {
                    "artist": artist,
                    "album_name": album_name,
                    "song_name": song_name,
                    "release_date": "",
                    "label": "",
                    "row_type": "track",
                    "discovery_source": "georgeclinton",
                    "source_url": detail_url,
                    "source_confidence": "high",
                    "raw_extra": json.dumps({"detail_url": detail_url, "track_number": track_num}),
                }
            )

        progress.update(extra=f"| {album_name[:40]}: {len(seen_tracks)} tracks")
    progress.finish()

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape georgeclinton.com/music for P-Funk release discovery."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch all pages ignoring cache",
    )
    parser.add_argument(
        "--force-page",
        metavar="URL",
        help="Re-fetch only this specific URL",
    )
    args = parser.parse_args()

    rows = run(force=args.force, force_page=args.force_page)

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

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    albums = sum(1 for r in rows if r["row_type"] == "album")
    tracks = sum(1 for r in rows if r["row_type"] == "track")
    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")
    print(f"  albums: {albums}, tracks: {tracks}")


if __name__ == "__main__":
    main()
