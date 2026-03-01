#!/usr/bin/env python3
"""
Scrape the P-Funk motherpage (list-albums.html) to discover releases.

Parses the fixed-width text listing organized by artist sections, extracts
album entries with year, title, label, and catalog number, and outputs
to discovery_raw/motherpage.csv for downstream reconciliation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from time import time

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent.parent
BASE_URL = "https://mother.pfunkarchive.com/motherpage/list-albums.html"
CACHE_DIR = ROOT / "data" / ".discovery_cache" / "mother.pfunkarchive.com"
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "motherpage.csv"
CACHE_TTL_SECONDS = 7 * 24 * 60 * 60

USER_AGENT = (
    "Mozilla/5.0 (compatible; pfunk-archive-discovery/1.0; +https://github.com/pfunkarchive)"
)


def cache_path_for_url(url: str) -> Path:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / h


def fetch_page(url: str, force: bool = False) -> tuple[str, int]:
    cache_path = cache_path_for_url(url)
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
        json.dumps(
            {
                "url": url,
                "timestamp": time(),
                "status_code": resp.status_code,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return html, resp.status_code


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
        stripped = line.strip()

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


def parse_section(artist: str, album_lines: list[str]) -> list[dict]:
    rows: list[dict] = []
    prev_album: str | None = None
    prev_label = ""
    for line in album_lines:
        parsed = parse_album_line(line, prev_album, prev_label)
        if parsed:
            prev_album = parsed["album_name"]
            prev_label = parsed["label"]
            rows.append(
                {
                    "artist": artist,
                    "album_name": parsed["album_name"],
                    "song_name": "",
                    "release_date": parsed["release_date"],
                    "label": parsed["label"],
                    "row_type": "album",
                    "discovery_source": "motherpage",
                    "source_url": BASE_URL,
                    "source_confidence": "high",
                    "raw_extra": json.dumps(parsed["raw_extra"]) if parsed["raw_extra"] else "",
                }
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape motherpage for P-Funk releases")
    parser.add_argument("--force", action="store_true", help="Re-fetch all pages ignoring cache")
    parser.add_argument("--force-page", metavar="URL", help="Re-fetch only this URL")
    args = parser.parse_args()

    force = args.force
    if args.force_page:
        force = True
    url = args.force_page if args.force_page else BASE_URL

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    html, status = fetch_page(url, force=force)
    if status != 200:
        raise SystemExit(f"Fetch failed: HTTP {status}")

    text = extract_text(html)
    sections = parse_sections(text)
    all_rows: list[dict] = []
    for artist, lines in sections:
        all_rows.extend(parse_section(artist, lines))

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
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    artists = len({r["artist"] for r in all_rows})
    print(f"Wrote {len(all_rows)} album rows to {OUTPUT_PATH}")
    print(f"Artists: {artists}")


if __name__ == "__main__":
    main()
