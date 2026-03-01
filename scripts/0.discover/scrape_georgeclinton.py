#!/usr/bin/env python3
"""
Scrape https://georgeclinton.com to discover P-Funk releases.

Level 0: /music/ index page -- album titles and detail links.
Level 1: /audio/<slug>/ detail pages -- track listings.
Level 2+: follow internal links for related releases, news posts, or bio
pages that reference albums.  Crawl depth is configurable via --max-depth.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from collections import deque
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

_RELEVANT_PATH_PREFIXES = ("/audio/", "/music/", "/news/", "/bio/", "/blog/")

_SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".ico",
    ".pdf", ".zip", ".mp3", ".wav", ".ogg", ".mp4", ".avi",
    ".css", ".js",
}


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

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
    except (json.JSONDecodeError, KeyError, ValueError):
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
        json.dumps({
            "url": url,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status_code": status_code,
        }, indent=2),
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


# ---------------------------------------------------------------------------
# Link extraction
# ---------------------------------------------------------------------------

def _is_relevant_link(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc != "georgeclinton.com" and parsed.netloc != "www.georgeclinton.com":
        return False
    path = parsed.path.rstrip("/").lower()
    if not path:
        return False
    if any(path.endswith(ext) for ext in _SKIP_EXTENSIONS):
        return False
    if any(path.startswith(prefix) for prefix in _RELEVANT_PATH_PREFIXES):
        if path == "/audio" or path == "/music":
            return False
        return True
    return False


def extract_links(html: str, current_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        absolute = urljoin(current_url, href).split("#")[0].rstrip("/")
        if not absolute:
            continue
        if _is_relevant_link(absolute):
            links.append(absolute)
    return links


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_artist_from_title(title: str) -> str:
    format_suffixes = ("single", "live", "ep")
    for sep in (" – ", " - "):
        if sep in title:
            parts = title.split(sep, 1)
            if len(parts) == 2 and parts[1].strip().lower() not in format_suffixes:
                if parts[0].strip():
                    return parts[0].strip()
    return "George Clinton"


def _split_artist_album(raw_title: str) -> tuple[str, str]:
    artist = parse_artist_from_title(raw_title)
    album_name = raw_title
    for sep in (" – ", " - "):
        if sep in raw_title:
            parts = raw_title.split(sep, 1)
            if parts[1].strip().lower() not in ("single", "live", "ep"):
                album_name = parts[-1].strip()
            else:
                album_name = parts[0].strip()
            break
    return artist, album_name or raw_title


def extract_album_entries(soup: BeautifulSoup) -> list[tuple[str, str]]:
    """Extract (title, detail_url) pairs from the /music/ index page."""
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


def extract_tracks(html: str) -> list[tuple[str, int | None]]:
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
            tracks.append((name, int(num_str)))

    if not tracks:
        for elem in soup.find_all("ol"):
            for i, li in enumerate(elem.find_all("li", recursive=False), 1):
                name = li.get_text(strip=True)
                if name and len(name) > 1:
                    tracks.append((name, i))

    return tracks


def parse_music_index(html: str, session: requests.Session, force: bool, force_page: str | None) -> list[dict[str, str]]:
    """Parse /music/ index and follow album detail links (level 0 + level 1)."""
    soup = BeautifulSoup(html, "html.parser")
    entries = extract_album_entries(soup)
    rows: list[dict[str, str]] = []

    for album_title, detail_url in entries:
        artist, album_name = _split_artist_album(album_title)

        rows.append({
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
        })

    return rows


def parse_audio_detail(html: str, url: str) -> list[dict[str, str]]:
    """Parse an /audio/<slug>/ detail page for tracks."""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []

    heading = soup.find(["h1", "h2"])
    raw_title = heading.get_text(strip=True) if heading else ""
    if not raw_title:
        path = urlparse(url).path.rstrip("/")
        raw_title = path.split("/")[-1].replace("-", " ").title()

    artist, album_name = _split_artist_album(raw_title)

    seen_tracks: set[tuple[str, int | None]] = set()
    for song_name, track_num in extract_tracks(html):
        key = (song_name, track_num)
        if key in seen_tracks:
            continue
        seen_tracks.add(key)
        rows.append({
            "artist": artist,
            "album_name": album_name,
            "song_name": song_name,
            "release_date": "",
            "label": "",
            "row_type": "track",
            "discovery_source": "georgeclinton",
            "source_url": url,
            "source_confidence": "high",
            "raw_extra": json.dumps({"detail_url": url, "track_number": track_num}),
        })

    if not rows:
        rows.append({
            "artist": artist,
            "album_name": album_name,
            "song_name": "",
            "release_date": "",
            "label": "",
            "row_type": "album",
            "discovery_source": "georgeclinton",
            "source_url": url,
            "source_confidence": "medium",
            "raw_extra": json.dumps({"detail_url": url}),
        })

    return rows


def parse_generic_page(html: str, url: str) -> list[dict[str, str]]:
    """Extract any discoverable data from a news/bio/blog page."""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []

    for heading in soup.find_all(["h1", "h2", "h3"]):
        text = heading.get_text(strip=True)
        if not text or len(text) < 4:
            continue
        for sep in (" – ", " - "):
            if sep in text:
                parts = text.split(sep, 1)
                artist = parts[0].strip()
                album_name = parts[1].strip()
                if artist and album_name and len(album_name) > 2:
                    rows.append({
                        "artist": artist,
                        "album_name": album_name,
                        "song_name": "",
                        "release_date": "",
                        "label": "",
                        "row_type": "album",
                        "discovery_source": "georgeclinton",
                        "source_url": url,
                        "source_confidence": "low",
                        "raw_extra": json.dumps({"page_type": "generic"}),
                    })
                break

    return rows


# ---------------------------------------------------------------------------
# BFS crawl
# ---------------------------------------------------------------------------

def crawl(
    seed_url: str,
    max_depth: int,
    session: requests.Session,
    force: bool,
    force_page: str | None,
) -> list[dict[str, str]]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, str]] = []
    queue: deque[tuple[str, int]] = deque([(seed_url, 0)])
    visited: set[str] = set()

    while queue:
        url, depth = queue.popleft()
        normalized = url.rstrip("/")
        if normalized in visited:
            continue
        visited.add(normalized)

        try:
            html, status = fetch(url, session, force=force, force_page=force_page)
        except Exception as exc:
            print(f"  [georgeclinton] fetch error {url}: {exc}")
            continue

        if status != 200:
            continue

        path = urlparse(url).path.rstrip("/").lower()
        if depth == 0 and url == seed_url:
            rows = parse_music_index(html, session, force, force_page)
        elif path.startswith("/audio/") and path != "/audio":
            rows = parse_audio_detail(html, url)
        else:
            rows = parse_generic_page(html, url)

        all_rows.extend(rows)
        print(f"  [georgeclinton] depth={depth} url={url} rows={len(rows)}")

        if depth < max_depth:
            for link in extract_links(html, url):
                link_norm = link.rstrip("/")
                if link_norm not in visited:
                    queue.append((link, depth + 1))

        time.sleep(RATE_LIMIT_SECONDS)

    return all_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape georgeclinton.com for P-Funk release discovery."
    )
    parser.add_argument("--force", action="store_true", help="Re-fetch all pages ignoring cache")
    parser.add_argument("--force-page", metavar="URL", help="Re-fetch only this specific URL")
    parser.add_argument("--max-depth", type=int, default=3,
                        help="Max crawl depth (default 3, 0 = seed page only)")
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    rows = crawl(
        seed_url=MUSIC_URL,
        max_depth=args.max_depth,
        session=session,
        force=args.force,
        force_page=args.force_page,
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    albums = sum(1 for r in rows if r["row_type"] == "album")
    tracks = sum(1 for r in rows if r["row_type"] == "track")
    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")
    print(f"  albums: {albums}, tracks: {tracks}, depth: {args.max_depth}")


if __name__ == "__main__":
    main()
