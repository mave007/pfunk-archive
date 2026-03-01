#!/usr/bin/env python3
"""
Scrape the Wikipedia List of P-Funk projects page to discover P-Funk releases.
Extracts artist, album, date, label from the chronological list and writes
to data/discovery_raw/wikipedia.csv for downstream catalog building.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parent.parent.parent
SOURCE_URL = "https://en.wikipedia.org/wiki/List_of_P-Funk_projects"
CACHE_DIR = ROOT / "data" / ".discovery_cache" / "en.wikipedia.org"
CACHE_TTL_DAYS = 7
OUTPUT_PATH = ROOT / "data" / "discovery_raw" / "wikipedia.csv"
OUTPUT_COLS = [
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

USER_AGENT = (
    "pfunk-archive/1.0 (https://github.com/pfunk-archive; "
    "+https://github.com/pfunk-archive/pfunk-archive)"
)


def cache_path(url: str) -> Path:
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{digest}.html"


def cache_is_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age_days = (time.time() - path.stat().st_mtime) / 86400
    return age_days < CACHE_TTL_DAYS


def fetch_page(
    url: str,
    force: bool = False,
    force_url: str | None = None,
) -> tuple[str, bool]:
    path = cache_path(url)
    path.parent.mkdir(parents=True, exist_ok=True)
    skip_cache = force or (force_url and force_url == url)
    if not skip_cache and path.exists() and cache_is_fresh(path):
        return path.read_text(encoding="utf-8"), False
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    html = resp.text
    path.write_text(html, encoding="utf-8")
    return html, True


def extract_date_from_heading(heading: str) -> tuple[str | None, str | None]:
    heading = heading.strip()
    if not heading:
        return None, None
    year_match = re.match(r"^(\d{4})$", heading)
    if year_match:
        return year_match.group(1), None
    month_day_match = re.match(
        r"^(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{1,2})$",
        heading,
        re.IGNORECASE,
    )
    if month_day_match:
        month, day = month_day_match.groups()
        months = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        mm = months.get(month.lower(), "01")
        return None, f"{mm}-{int(day):02d}"
    month_only = re.match(
        r"^(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)$",
        heading,
        re.IGNORECASE,
    )
    if month_only:
        months = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        return None, f"{months.get(month_only.group(1).lower(), '01')}-01"
    return None, None


def parse_release_line(
    text: str,
    current_year: str | None,
    current_month_day: str | None,
) -> dict | None:
    text = re.sub(r"\s+", " ", text).strip()
    if not text or len(text) < 10:
        return None
    sep = " - "
    candidates = [i for i in range(len(text) - len(sep) + 1) if text[i : i + len(sep)] == sep]
    dash_idx = -1
    for i in reversed(candidates):
        after = text[i + len(sep) :].strip()
        if after.startswith("[") or after.startswith('"'):
            dash_idx = i
            break
    if dash_idx < 0:
        return None
    artist = text[:dash_idx].strip()
    rest = text[dash_idx + len(sep) :].strip()
    if not artist or not rest:
        return None
    artist = re.sub(r"\[([^\]]+)\]", r"\1", artist)
    artist = re.sub(
        r"^(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
        r"Nov(?:ember)?|Dec(?:ember)?)(\s+\d{1,2})?\s+",
        "",
        artist,
        flags=re.IGNORECASE,
    ).strip()
    if " - " in artist and "(" in artist:
        parts = artist.split(" - ")
        for p in reversed(parts):
            p = p.strip()
            if len(p) < 50 and "(" not in p and p:
                artist = p
                break
    if len(artist) > 80 or re.search(r"\b(record|demos)\b", artist, re.I):
        return None
    title = ""
    label = ""
    raw_extra = {}
    link_match = re.search(r"\[([^\]]+)\]\([^)]+\)", rest)
    if link_match:
        title = link_match.group(1)
    else:
        quote_match = re.search(r'"([^"]+)"(?:"/([^"]+)")?', rest)
        if quote_match:
            a = quote_match.group(1)
            b = quote_match.group(2)
            title = f"{a}/{b}" if b else a
    if not title:
        return None
    format_match = re.search(r"\((LP|CD|7\"|10\"|12\"|CD single)\)", rest)
    format_type = format_match.group(1) if format_match else ""
    parens = re.findall(r"\(([^()]+)\)", rest)
    for p in parens:
        if re.match(r"^(LP|CD|7\"|10\"|12\"|CD single)$", p):
            continue
        if any(
            x in p.lower()
            for x in ["records", "westbound", "casablanca", "warner", "atlantic", "invictus"]
        ):
            label = p.strip()
            break
        if re.search(r"[A-Z]{2,}\s*\d+|[A-Z]+\d{4,}", p) and not label:
            raw_extra["catalog_number"] = p
    if format_type:
        raw_extra["format"] = format_type
    release_date = current_year or ""
    if current_month_day and current_year:
        release_date = f"{current_year}-{current_month_day}"
    row_type = "album" if format_type in ("LP", "CD") else "single"
    return {
        "artist": artist,
        "album_name": title if row_type == "album" else "",
        "song_name": title if row_type == "single" else "",
        "release_date": release_date,
        "label": label,
        "row_type": row_type,
        "discovery_source": "wikipedia",
        "source_url": SOURCE_URL,
        "source_confidence": "medium",
        "raw_extra": json.dumps(raw_extra) if raw_extra else "",
    }


def parse_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", class_="mw-parser-output")
    if not content:
        content = soup.body
    if not content:
        return []
    current_year = None
    current_month_day = None
    rows = []
    for el in content.find_all(["h2", "h3", "h4", "li"]):
        if el.name in ("h2", "h3", "h4"):
            text = el.get_text(separator=" ", strip=True)
            text = re.sub(r"\[\d+\]", "", text).strip()
            if el.name == "h2":
                current_year = None
                current_month_day = None
            elif el.name in ("h3", "h4"):
                yr, md = extract_date_from_heading(text)
                if yr:
                    current_year = yr
                    current_month_day = None
                elif md:
                    current_month_day = md
            continue
        if el.name != "li":
            continue
        text = el.get_text(separator=" ", strip=True)
        text = re.sub(r"\[\d+\]", "", text)
        row = parse_release_line(text, current_year, current_month_day)
        if row:
            rows.append(row)
    seen = set()
    deduped = []
    for r in rows:
        key = (r["artist"], r["album_name"], r["song_name"], r["release_date"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Wikipedia List of P-Funk projects for discovery data."
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
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    html, from_network = fetch_page(
        SOURCE_URL,
        force=args.force,
        force_url=args.force_page,
    )
    rows = parse_page(html)
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(OUTPUT_COLS)
        for r in rows:
            w.writerow([r.get(c, "") for c in OUTPUT_COLS])
    print(f"Pages fetched: {1 if from_network else 0}")
    print(f"Rows extracted: {len(rows)}")


if __name__ == "__main__":
    main()
