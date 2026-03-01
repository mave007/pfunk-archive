#!/usr/bin/env python3
"""
Scrape the P-Funk motherpage site to discover releases.

Level 0: list-albums.html -- fixed-width text listing organized by artist.
Level 1+: follow internal links to detail pages for track lists, session
info, and liner notes.  Crawl depth is configurable via --max-depth.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time as _time
from collections import deque
from pathlib import Path
from time import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent.parent
SEED_URL = "https://mother.pfunkarchive.com/motherpage/list-albums.html"
BASE_DOMAIN = "mother.pfunkarchive.com"
CACHE_DIR = ROOT / "data" / ".discovery_cache" / "mother.pfunkarchive.com"
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "motherpage.csv"
CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
RATE_LIMIT_SECONDS = 1.5

USER_AGENT = (
    "Mozilla/5.0 (compatible; pfunk-archive-discovery/1.0; +https://github.com/pfunkarchive)"
)

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

_SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".ico",
    ".pdf", ".zip", ".mp3", ".wav", ".ogg", ".mp4", ".avi",
    ".css", ".js",
}


def _cache_path_for_url(url: str) -> Path:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / h


def fetch_page(url: str, force: bool = False) -> tuple[str, int]:
    cache_path = _cache_path_for_url(url)
    meta_path = cache_path.with_suffix(cache_path.suffix + ".meta.json")

    if not force and cache_path.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("status_code") == 200:
                age = time() - meta.get("timestamp", 0)
                if age < CACHE_TTL_SECONDS:
                    return cache_path.read_text(encoding="utf-8"), 200
        except (json.JSONDecodeError, OSError):
            pass

    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    html = resp.text
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(html, encoding="utf-8")
    meta_path.write_text(
        json.dumps({"url": url, "timestamp": time(), "status_code": resp.status_code}, indent=2),
        encoding="utf-8",
    )
    return html, resp.status_code


# ---------------------------------------------------------------------------
# Link extraction
# ---------------------------------------------------------------------------

def _is_same_domain(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc == BASE_DOMAIN or parsed.netloc == ""


def _should_skip_url(url: str) -> bool:
    parsed = urlparse(url)
    path_lower = parsed.path.lower()
    if any(path_lower.endswith(ext) for ext in _SKIP_EXTENSIONS):
        return True
    if parsed.fragment and not parsed.path:
        return True
    return False


def extract_links(html: str, current_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        absolute = urljoin(current_url, href)
        absolute = absolute.split("#")[0].rstrip("/")
        if not absolute or not _is_same_domain(absolute):
            continue
        if _should_skip_url(absolute):
            continue
        if absolute.startswith("mailto:") or absolute.startswith("javascript:"):
            continue
        links.append(absolute)
    return links


# ---------------------------------------------------------------------------
# Parsers for different page types
# ---------------------------------------------------------------------------

def extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    pre = soup.find("pre")
    if pre is not None:
        return pre.get_text(separator="\n")
    return soup.get_text(separator="\n")


def parse_sections(text: str) -> list[tuple[str, list[str]]]:
    lines = text.splitlines()
    sections: list[tuple[str, list[str]]] = []
    current_artist: str | None = None
    album_lines: list[str] = []
    i = 0

    def is_dash_line(s: str) -> bool:
        return bool(s) and all(c in "-=*" for c in s.strip())

    def flush():
        nonlocal album_lines, current_artist
        if current_artist and album_lines:
            sections.append((current_artist, album_lines))
        album_lines = []

    while i < len(lines):
        line = lines[i]

        if is_dash_line(line):
            if i + 1 < len(lines):
                artist_line = lines[i + 1].strip()
                if artist_line and not is_dash_line(artist_line):
                    flush()
                    current_artist = artist_line
                    album_lines = []
                    i += 2
                    if i < len(lines) and is_dash_line(lines[i]):
                        i += 1
                    continue

        stripped = line.strip()
        if current_artist and stripped and not is_dash_line(line):
            asterisk_only = re.match(r"^\*+$", stripped)
            if not asterisk_only and "****" not in stripped:
                album_lines.append(line)
        i += 1

    flush()
    return sections


def _parse_date_from_format(s: str) -> str:
    m = re.search(r"\([^)]*(\d{2})/(\d{1,2})/(\d{2})\)", s)
    if m:
        yy = int(m.group(3))
        return str(1900 + yy) if yy >= 70 else str(2000 + yy)
    m = re.search(r"\([^)]*(\d{4})\)", s)
    if m:
        return m.group(1)
    m = re.search(r"\([^)]*(\d{2})\)", s)
    if m:
        yy = int(m.group(1))
        return str(1900 + yy) if yy >= 70 else str(2000 + yy)
    return ""


def parse_album_line(line: str, prev_album: str | None, prev_label: str = "") -> dict | None:
    line = line.rstrip()
    if not line or line.isspace():
        return None

    continuation = line.lstrip().startswith('"')
    if continuation and prev_album:
        title = prev_album
        rest = line.lstrip()
        m = re.match(r'^["\s]+(.*)$', rest)
        work = (m.group(1) if m else rest).strip()
    else:
        m = re.match(r"^(.+?)\s{2,}(\d{2}[\^*]?)\s+(.+)$", line)
        if not m:
            return None
        title = m.group(1).strip()
        year_part = m.group(2)
        work = m.group(3).strip()

    release_date = ""
    if not continuation:
        year_match = re.match(r"(\d{2})[\^*]?", year_part)
        if year_match:
            y = int(year_match.group(1))
            release_date = str(1900 + y) if y >= 70 else str(2000 + y)
    else:
        release_date = _parse_date_from_format(work)

    paren_matches = re.findall(r"\(([^)]+)\)", work)
    format_note = " ".join(paren_matches) if paren_matches else ""
    work_rest = re.sub(r"\([^)]+\)", " ", work)
    work_rest = re.sub(r"^[\s/]+", "", work_rest).strip()

    parts = re.split(r"\s{2,}", work_rest, maxsplit=1)
    if len(parts) >= 2:
        label = parts[0].strip()
        catalog = parts[1].strip()
    elif continuation and work_rest and re.search(r"\d", work_rest):
        label = prev_label
        catalog = work_rest.strip()
    else:
        tok = work_rest.split()
        label = tok[0] if tok else ""
        catalog = " ".join(tok[1:]) if len(tok) > 1 else ""

    extra: dict = {}
    if catalog:
        extra["catalog_number"] = catalog
    if format_note:
        extra["format_note"] = format_note

    return {
        "album_name": title,
        "release_date": release_date,
        "label": label,
        "raw_extra": extra,
    }


def parse_album_listing_page(html: str, source_url: str) -> list[dict]:
    """Parse the main list-albums.html page (fixed-width <pre> text)."""
    text = extract_text(html)
    sections = parse_sections(text)
    rows: list[dict] = []
    for artist, album_lines in sections:
        prev_album: str | None = None
        prev_label = ""
        for line in album_lines:
            parsed = parse_album_line(line, prev_album, prev_label)
            if parsed:
                prev_album = parsed["album_name"]
                prev_label = parsed["label"]
                rows.append({
                    "artist": artist,
                    "album_name": parsed["album_name"],
                    "song_name": "",
                    "release_date": parsed["release_date"],
                    "label": parsed["label"],
                    "row_type": "album",
                    "discovery_source": "motherpage",
                    "source_url": source_url,
                    "source_confidence": "high",
                    "raw_extra": json.dumps(parsed["raw_extra"]) if parsed["raw_extra"] else "",
                })
    return rows


def parse_detail_page(html: str, source_url: str) -> list[dict]:
    """Extract discoverable data from a sub-page (tracks, credits, etc.)."""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []

    title_tag = soup.find("title")
    page_title = title_tag.get_text(strip=True) if title_tag else ""

    artist = ""
    album_name = ""
    for heading in soup.find_all(["h1", "h2", "h3"]):
        text = heading.get_text(strip=True)
        if not text or len(text) < 3:
            continue
        for sep in (" – ", " - "):
            if sep in text:
                parts = text.split(sep, 1)
                artist = parts[0].strip()
                album_name = parts[1].strip()
                break
        if artist:
            break
        if not album_name:
            album_name = text

    if not artist and page_title:
        for sep in (" – ", " - ", " | "):
            if sep in page_title:
                parts = page_title.split(sep, 1)
                artist = artist or parts[0].strip()
                album_name = album_name or parts[1].strip()
                break

    if not album_name:
        return rows

    for elem in soup.find_all("ol"):
        for i, li in enumerate(elem.find_all("li", recursive=False), 1):
            song = li.get_text(strip=True)
            if song and len(song) > 1:
                rows.append({
                    "artist": artist,
                    "album_name": album_name,
                    "song_name": song,
                    "release_date": "",
                    "label": "",
                    "row_type": "track",
                    "discovery_source": "motherpage",
                    "source_url": source_url,
                    "source_confidence": "medium",
                    "raw_extra": json.dumps({"page_title": page_title}),
                })

    if not rows:
        for elem in soup.find_all(["p", "div", "li"]):
            text = elem.get_text(separator=" ", strip=True)
            if not text or len(text) < 5:
                continue
            pattern = r'(\d{1,2})\.\s*["\u201c\u201d]?([^"\d]+?)["\u201c\u201d]?(?=\s*\d{1,2}\.\s|$)'
            matches = re.findall(pattern, text)
            for _, name in matches:
                name = name.strip().strip('"\u201c\u201d')
                if len(name) >= 2:
                    rows.append({
                        "artist": artist,
                        "album_name": album_name,
                        "song_name": name,
                        "release_date": "",
                        "label": "",
                        "row_type": "track",
                        "discovery_source": "motherpage",
                        "source_url": source_url,
                        "source_confidence": "medium",
                        "raw_extra": json.dumps({"page_title": page_title}),
                    })

    if album_name and not rows:
        rows.append({
            "artist": artist,
            "album_name": album_name,
            "song_name": "",
            "release_date": "",
            "label": "",
            "row_type": "album",
            "discovery_source": "motherpage",
            "source_url": source_url,
            "source_confidence": "low",
            "raw_extra": json.dumps({"page_title": page_title}),
        })

    return rows


# ---------------------------------------------------------------------------
# BFS crawl
# ---------------------------------------------------------------------------

def crawl(
    seed_url: str,
    max_depth: int,
    force: bool,
    force_page: str | None,
) -> list[dict]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    queue: deque[tuple[str, int]] = deque([(seed_url, 0)])
    visited: set[str] = set()

    while queue:
        url, depth = queue.popleft()
        normalized = url.rstrip("/")
        if normalized in visited:
            continue
        visited.add(normalized)

        page_force = force or (force_page is not None and url == force_page)
        try:
            html, status = fetch_page(url, force=page_force)
        except Exception as exc:
            print(f"  [motherpage] fetch error {url}: {exc}")
            continue

        if status != 200:
            continue

        if depth == 0 and url == seed_url:
            rows = parse_album_listing_page(html, url)
        else:
            rows = parse_detail_page(html, url)

        all_rows.extend(rows)
        print(f"  [motherpage] depth={depth} url={url} rows={len(rows)}")

        if depth < max_depth:
            for link in extract_links(html, url):
                link_norm = link.rstrip("/")
                if link_norm not in visited:
                    queue.append((link, depth + 1))

        _time.sleep(RATE_LIMIT_SECONDS)

    return all_rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape motherpage for P-Funk releases")
    parser.add_argument("--force", action="store_true", help="Re-fetch all pages ignoring cache")
    parser.add_argument("--force-page", metavar="URL", help="Re-fetch only this URL")
    parser.add_argument("--max-depth", type=int, default=3,
                        help="Max crawl depth (default 3, 0 = seed page only)")
    args = parser.parse_args()

    all_rows = crawl(
        seed_url=SEED_URL,
        max_depth=args.max_depth,
        force=args.force,
        force_page=args.force_page,
    )

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(all_rows)

    artists = len({r["artist"] for r in all_rows if r["artist"]})
    print(f"Wrote {len(all_rows)} rows to {OUTPUT_PATH}")
    print(f"Artists: {artists}, depth: {args.max_depth}")


if __name__ == "__main__":
    main()
