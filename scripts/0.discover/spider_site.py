#!/usr/bin/env python3
"""
Generic site spider that crawls entire domains to find content that targeted scrapers miss.

Uses BFS crawling with configurable depth, respects robots.txt, caches all fetched pages
to data/.discovery_cache/<domain>/, and supports domain-specific extraction profiles.
Outputs sitemap.json with discovered URLs; when --extract is set, also runs extraction
rules and writes CSV to data/discovery_raw/spider_<domain>.csv.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from collections import deque
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import sys

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import ProgressTracker  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent.parent

USER_AGENT = (
    "Mozilla/5.0 (compatible; pfunk-archive-spider/1.0; +https://github.com/mave007/pfunk-archive)"
)

CSV_FIELDS = [
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


def url_hash(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def get_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc or ""


def cache_dir_for_domain(domain: str) -> Path:
    return ROOT / "data" / ".discovery_cache" / domain.replace("/", "_")


def content_path(cache_dir: Path, url: str, is_json: bool = False) -> Path:
    ext = ".json" if is_json else ".html"
    return cache_dir / f"{url_hash(url)}{ext}"


def meta_path(cache_dir: Path, url: str) -> Path:
    return cache_dir / f"{url_hash(url)}.meta.json"


def load_robots(domain: str, session: requests.Session, cache_dir: Path) -> RobotFileParser:
    netloc = urlparse("https://" + domain if not domain.startswith("http") else domain).netloc
    robots_url = f"https://{netloc}/robots.txt"
    robots_path = cache_dir / "robots.txt"
    if robots_path.exists():
        rp = RobotFileParser()
        rp.parse(robots_path.read_text(encoding="utf-8").splitlines())
        return rp
    try:
        resp = session.get(robots_url, timeout=10)
        if resp.status_code == 200:
            cache_dir.mkdir(parents=True, exist_ok=True)
            robots_path.write_text(resp.text, encoding="utf-8")
    except Exception:
        pass
    rp = RobotFileParser()
    rp.parse([])
    return rp


def fetch_page(
    url: str,
    session: requests.Session,
    cache_dir: Path,
    force: bool,
    force_page: str | None,
) -> tuple[str | None, int, int]:
    mpath = meta_path(cache_dir, url)
    skip_cache = force or (force_page and url == force_page)
    if not skip_cache and mpath.exists():
        try:
            meta = json.loads(mpath.read_text(encoding="utf-8"))
            h = url_hash(url)
            for ext in (".html", ".json"):
                cpath = cache_dir / f"{h}{ext}"
                if cpath.exists():
                    return cpath.read_text(encoding="utf-8"), meta.get("depth", 0), meta.get("status_code", 200)
        except (json.JSONDecodeError, OSError):
            pass
    resp = session.get(url, timeout=30)
    body = resp.text
    is_json = "application/json" in (resp.headers.get("Content-Type") or "") or url.rstrip("/").endswith(".json")
    cache_dir.mkdir(parents=True, exist_ok=True)
    cpath = content_path(cache_dir, url, is_json)
    cpath.write_text(body, encoding="utf-8")
    mpath.write_text(
        json.dumps(
            {
                "url": url,
                "timestamp": time.time(),
                "status_code": resp.status_code,
                "depth": -1,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return body, -1, resp.status_code


def extract_links(html: str, base_url: str, domain: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue
        if any(href.lower().endswith(ext) for ext in (".gif", ".jpg", ".png", ".pdf", ".zip")):
            continue
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if parsed.netloc != domain:
            continue
        path = parsed.path or "/"
        if path.endswith((".gif", ".jpg", ".png")):
            continue
        frag = "" if parsed.fragment is None else "#"
        candidate = parsed._replace(fragment=frag, query="").geturl()
        if candidate.endswith("#"):
            candidate = candidate.rstrip("#")
        links.append(candidate)
    return links


def get_page_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("title")
    return title.get_text(strip=True) if title else ""


def crawl(
    seed_url: str,
    max_depth: int,
    max_pages: int,
    delay: float,
    force: bool,
    force_page: str | None,
    url_pattern: re.Pattern | None,
    exclude_pattern: re.Pattern | None,
) -> list[dict]:
    parsed_seed = urlparse(seed_url)
    domain = parsed_seed.netloc
    if not domain:
        raise SystemExit("Invalid seed URL: could not parse domain")
    cache_dir = cache_dir_for_domain(domain)
    cache_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    rp = load_robots(domain, session, cache_dir)
    if not rp.can_fetch(USER_AGENT, seed_url):
        raise SystemExit(f"Robots.txt disallows crawling {seed_url}")

    sitemap: list[dict] = []
    seen: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(seed_url, 0)])
    pages_fetched = 0
    progress = ProgressTracker(total=max_pages, noun="pages", every=5)

    while queue and pages_fetched < max_pages:
        url, depth = queue.popleft()
        if url in seen:
            continue
        seen.add(url)

        if url_pattern and not url_pattern.search(url):
            continue
        if exclude_pattern and exclude_pattern.search(url):
            continue

        if not rp.can_fetch(USER_AGENT, url):
            continue

        html, cached_depth, status = fetch_page(url, session, cache_dir, force, force_page)
        if html is None or status != 200:
            continue

        if cached_depth >= 0:
            depth = cached_depth
        else:
            for f in cache_dir.glob(f"{url_hash(url)}.meta.json"):
                try:
                    meta = json.loads(f.read_text(encoding="utf-8"))
                    meta["depth"] = depth
                    f.write_text(json.dumps(meta, indent=2), encoding="utf-8")
                except (json.JSONDecodeError, OSError):
                    pass
                break

        title = get_page_title(html)
        sitemap.append({"url": url, "title": title, "depth": depth})
        pages_fetched += 1

        progress.update(extra=f"| queue={len(queue)} discovered={len(seen)} depth={depth}")

        if depth >= max_depth:
            continue

        for link in extract_links(html, url, domain):
            if link not in seen and (url_pattern is None or url_pattern.search(link)) and (exclude_pattern is None or not exclude_pattern.search(link)):
                queue.append((link, depth + 1))

        time.sleep(delay)

    progress.done = pages_fetched
    progress.finish(extra=f"| discovered={len(seen)} URLs")
    return sitemap


def _content_from_meta(mpath: Path) -> tuple[str, str] | None:
    try:
        meta = json.loads(mpath.read_text(encoding="utf-8"))
        url = meta.get("url", "")
        base = mpath.stem.replace(".meta", "")
        for ext in (".html", ".json"):
            cpath = mpath.parent / (base + ext)
            if cpath.exists():
                return url, cpath.read_text(encoding="utf-8")
    except (json.JSONDecodeError, OSError, KeyError):
        pass
    return None


def extract_mother_pfunkarchive(domain: str, cache_dir: Path) -> list[dict]:
    rows: list[dict] = []
    source_name = f"spider_{domain}"
    for mpath in cache_dir.glob("*.meta.json"):
        pair = _content_from_meta(mpath)
        if not pair:
            continue
        url, html = pair
        if "motherpage" not in url.lower() or not url.endswith(".html"):
            continue

        soup = BeautifulSoup(html, "html.parser")
        pre = soup.find("pre")
        text = pre.get_text(separator="\n") if pre else soup.get_text(separator="\n")
        lines = text.splitlines()

        def is_dash(s: str) -> bool:
            return bool(s) and all(c in "-=*" for c in s.strip())

        sections: list[tuple[str, list[str]]] = []
        current_artist: str | None = None
        album_lines: list[str] = []

        def flush():
            nonlocal album_lines, current_artist
            if current_artist and album_lines:
                sections.append((current_artist, album_lines))
            album_lines = []

        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            if is_dash(line):
                if i + 1 < len(lines):
                    artist_line = lines[i + 1].strip()
                    if artist_line and not is_dash(artist_line):
                        flush()
                        current_artist = artist_line
                        album_lines = []
                        i += 2
                        if i < len(lines) and is_dash(lines[i]):
                            i += 1
                        continue
            if current_artist and stripped and not is_dash(line):
                if not re.match(r"^\*+$", stripped) and "****" not in stripped:
                    album_lines.append(line)
            i += 1
        flush()

        def parse_date(s: str) -> str:
            m = re.search(r"\([^)]*(\d{2})/(\d{1,2})/(\d{2})\)", s)
            if m:
                yy = int(m.group(3))
                return str(1900 + yy) if yy >= 70 else str(2000 + yy)
            m = re.search(r"\([^)]*(\d{4})\)", s)
            return m.group(1) if m else ""

        prev_album: str | None = None
        prev_label = ""
        for artist, album_lines_section in sections:
            for line in album_lines_section:
                line = line.rstrip()
                if not line or line.isspace():
                    continue
                continuation = line.lstrip().startswith('"')
                if continuation and prev_album:
                    title = prev_album
                    rest = line.lstrip()
                    m = re.match(r'^["\s]+(.*)$', rest)
                    work = (m.group(1) if m else rest).strip()
                    year_part = ""
                else:
                    m = re.match(r"^(.+?)\s{2,}(\d{2}[\^*]?)\s+(.+)$", line)
                    if not m:
                        continue
                    title = m.group(1).strip()
                    year_part = m.group(2)
                    work = m.group(3).strip()
                year_m = re.match(r"(\d{2})[\^*]?", year_part) if year_part else None
                release_date = str(1900 + int(year_m.group(1))) if year_m and int(year_m.group(1)) >= 70 else (str(2000 + int(year_m.group(1))) if year_m else parse_date(work))
                work_rest = re.sub(r"\([^)]+\)", " ", work).strip()
                parts = re.split(r"\s{2,}", work_rest, maxsplit=1)
                label = parts[0].strip() if parts else ""
                if continuation:
                    label = prev_label
                prev_album = title
                prev_label = label
                extra: dict = {}
                rows.append({
                    "artist": artist,
                    "album_name": title,
                    "song_name": "",
                    "release_date": release_date,
                    "label": label,
                    "row_type": "album",
                    "discovery_source": source_name,
                    "source_url": url,
                    "source_confidence": "high",
                    "raw_extra": json.dumps(extra) if extra else "",
                })

        if "sessionwork" in url.lower():
            for artist, _ in sections:
                rows.append({
                    "artist": artist,
                    "album_name": "",
                    "song_name": "",
                    "release_date": "",
                    "label": "",
                    "row_type": "session_credit",
                    "discovery_source": source_name,
                    "source_url": url,
                    "source_confidence": "medium",
                    "raw_extra": json.dumps({"section": "sessionwork"}),
                })

        if "list-singles" in url.lower():
            for artist, album_lines_section in sections:
                for line in album_lines_section:
                    if line.strip():
                        rows.append({
                            "artist": artist,
                            "album_name": "",
                            "song_name": line.strip()[:200],
                            "release_date": "",
                            "label": "",
                            "row_type": "single",
                            "discovery_source": source_name,
                            "source_url": url,
                            "source_confidence": "medium",
                            "raw_extra": "",
                        })

    return rows


def extract_georgeclinton(domain: str, cache_dir: Path) -> list[dict]:
    rows: list[dict] = []
    source_name = f"spider_{domain}"
    base = "https://georgeclinton.com"
    for mpath in cache_dir.glob("*.meta.json"):
        pair = _content_from_meta(mpath)
        if not pair:
            continue
        url, content = pair
        if "/audio/" not in url and "/music/" not in url:
            continue

        soup = BeautifulSoup(content, "html.parser")
        if "/audio/" in url:
            for elem in soup.find_all(["p", "div", "li"]):
                text = elem.get_text(separator=" ", strip=True)
                if not text or len(text) < 3:
                    continue
                pattern = r'(\d{1,2})\.\s*["\u201c\u201d]?([^"\d]+?)["\u201c\u201d]?(?=\s*\d{1,2}\.\s|$)'
                for num_str, name in re.findall(pattern, text):
                    name = name.strip().strip('"\u201c\u201d')
                    if len(name) < 2:
                        continue
                    artist = "George Clinton"
                    if " – " in name or " - " in name:
                        parts = name.split(" – ", 1) if " – " in name else name.split(" - ", 1)
                        artist = parts[0].strip()
                        name = parts[1].strip() if len(parts) > 1 else name
                    album = url.replace(base, "").strip("/").replace("/", " ").replace("-", " ").title() or "Unknown"
                    rows.append({
                        "artist": artist,
                        "album_name": album,
                        "song_name": name,
                        "release_date": "",
                        "label": "",
                        "row_type": "track",
                        "discovery_source": source_name,
                        "source_url": url,
                        "source_confidence": "high",
                        "raw_extra": json.dumps({"detail_url": url}),
                    })
        else:
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "/audio/" not in href:
                    continue
                detail_url = urljoin(base, href)
                title = a.get_text(strip=True).replace("Read more", "").strip()
                if not title or len(title) < 2:
                    continue
                artist = "George Clinton"
                album_name = title
                for sep in (" – ", " - "):
                    if sep in title:
                        parts = title.split(sep, 1)
                        if parts[1].strip().lower() not in ("single", "live", "ep"):
                            album_name = parts[-1].strip()
                        break
                rows.append({
                    "artist": artist,
                    "album_name": album_name,
                    "song_name": "",
                    "release_date": "",
                    "label": "",
                    "row_type": "album",
                    "discovery_source": source_name,
                    "source_url": detail_url,
                    "source_confidence": "high",
                    "raw_extra": json.dumps({"detail_url": detail_url}),
                })

    return rows


def extract_pfunkforums(domain: str, cache_dir: Path) -> list[dict]:
    rows: list[dict] = []
    source_name = f"spider_{domain}"
    base = "https://pfunkforums.com"
    for mpath in cache_dir.glob("*.meta.json"):
        pair = _content_from_meta(mpath)
        if not pair:
            continue
        url, content = pair
        if ".json" not in url or "/c/" not in url:
            continue
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            continue

        topic_list = data.get("topic_list") or {}
        topics = topic_list.get("topics") or []
        for t in topics:
            tid = t.get("id")
            slug = t.get("slug", "")
            title = t.get("title", "")
            tags = t.get("tags") or []
            source_url = f"{base}/t/{slug}/{tid}" if tid and slug else url
            non_meta = [x for x in tags if x.lower() not in ("book", "7-inch", "12-inch", "magazine", "comics", "bootleg", "stream")]
            artist = ", ".join(x.replace("-", " ").title() for x in non_meta) if non_meta else ""
            rows.append({
                "artist": artist,
                "album_name": title,
                "song_name": "",
                "release_date": "",
                "label": "",
                "row_type": "album",
                "discovery_source": source_name,
                "source_url": source_url,
                "source_confidence": "low",
                "raw_extra": json.dumps({"tags": tags, "views": t.get("views"), "reply_count": t.get("reply_count")}),
            })

    return rows


def extract_wikipedia(domain: str, cache_dir: Path) -> list[dict]:
    rows: list[dict] = []
    source_name = f"spider_{domain}"
    for mpath in cache_dir.glob("*.meta.json"):
        pair = _content_from_meta(mpath)
        if not pair:
            continue
        url, content = pair
        if "/wiki/" not in url:
            continue

        soup = BeautifulSoup(content, "html.parser")
        infobox = soup.find("table", class_=re.compile(r"infobox", re.I))
        if not infobox:
            continue
        release_date = ""
        label = ""
        genre = ""
        for tr in infobox.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True).lower()
            val = td.get_text(strip=True)
            if "release" in key or "date" in key:
                release_date = val[:50]
            elif "label" in key:
                label = val[:100]
            elif "genre" in key:
                genre = val[:100]
        title_el = soup.find("h1", class_="mw-page-title-main") or soup.find("h1")
        album_name = title_el.get_text(strip=True) if title_el else ""
        if not album_name or len(album_name) < 2:
            continue
        extra: dict = {}
        if genre:
            extra["genre"] = genre
        rows.append({
            "artist": "",
            "album_name": album_name,
            "song_name": "",
            "release_date": release_date,
            "label": label,
            "row_type": "album",
            "discovery_source": source_name,
            "source_url": url,
            "source_confidence": "medium",
            "raw_extra": json.dumps(extra) if extra else "",
        })

    return rows


DOMAIN_URL_PATTERNS: dict[str, tuple[str | None, str | None]] = {
    "mother.pfunkarchive.com": (r"motherpage/.*\.html$", r"mailto:|\.gif|\.jpg|\.png"),
    "georgeclinton.com": (r"/audio/|/music/", None),
    "pfunkforums.com": (r"/c/[^/]+/\d+\.json", None),
    "en.wikipedia.org": (r"/wiki/", None),
}

EXTRACTORS: dict[str, callable] = {
    "mother.pfunkarchive.com": extract_mother_pfunkarchive,
    "georgeclinton.com": extract_georgeclinton,
    "pfunkforums.com": extract_pfunkforums,
    "en.wikipedia.org": extract_wikipedia,
}


def run_extraction(domain: str, cache_dir: Path) -> list[dict]:
    fn = EXTRACTORS.get(domain)
    if fn:
        return fn(domain, cache_dir)
    return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl a domain from a seed URL and optionally extract discovery data."
    )
    parser.add_argument("seed_url", help="Starting URL to crawl")
    parser.add_argument("--max-depth", type=int, default=3, metavar="N", help="Maximum link depth (default: 3)")
    parser.add_argument("--max-pages", type=int, default=500, metavar="N", help="Maximum pages to fetch (default: 500)")
    parser.add_argument("--delay", type=float, default=2.0, metavar="SECONDS", help="Delay between requests (default: 2.0)")
    parser.add_argument("--force", action="store_true", help="Re-fetch all pages, ignoring cache")
    parser.add_argument("--force-page", metavar="URL", help="Re-fetch only a specific cached page")
    parser.add_argument("--url-pattern", metavar="REGEX", help="Only follow URLs matching this pattern")
    parser.add_argument("--exclude-pattern", metavar="REGEX", help="Skip URLs matching this pattern")
    parser.add_argument("--extract", action="store_true", help="Run extraction rules after crawl")
    args = parser.parse_args()

    seed = args.seed_url.strip()
    if not seed.startswith(("http://", "https://")):
        seed = "https://" + seed

    domain = get_domain(seed)
    safe_domain = domain.replace("/", "_")

    url_pat = re.compile(args.url_pattern) if args.url_pattern else None
    exclude_pat = re.compile(args.exclude_pattern) if args.exclude_pattern else None
    if url_pat is None or exclude_pat is None:
        domain_defaults = DOMAIN_URL_PATTERNS.get(domain, (None, None))
        if url_pat is None and domain_defaults[0]:
            url_pat = re.compile(domain_defaults[0])
        if exclude_pat is None and domain_defaults[1]:
            exclude_pat = re.compile(domain_defaults[1])
    cache_dir = cache_dir_for_domain(domain)
    sitemap_path = ROOT / "data" / "discovery_raw" / f"spider_{safe_domain}_sitemap.json"
    csv_path = ROOT / "data" / "discovery_raw" / f"spider_{safe_domain}.csv"

    (ROOT / "data" / "discovery_raw").mkdir(parents=True, exist_ok=True)

    print(f"Crawling {seed} (domain: {domain})")
    sitemap = crawl(
        seed,
        max_depth=args.max_depth,
        max_pages=args.max_pages,
        delay=args.delay,
        force=args.force,
        force_page=args.force_page,
        url_pattern=url_pat,
        exclude_pattern=exclude_pat,
    )

    sitemap_path.write_text(json.dumps(sitemap, indent=2), encoding="utf-8")
    print(f"\nSummary:")
    print(f"  Pages crawled: {len(sitemap)}")
    print(f"  URLs discovered: {len(sitemap)}")
    print(f"  Sitemap: {sitemap_path}")

    rows: list[dict] = []
    if args.extract:
        print("\nRunning extraction...")
        rows = run_extraction(domain, cache_dir)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()
            w.writerows(rows)
        print(f"  Rows extracted: {len(rows)}")
        print(f"  CSV: {csv_path}")


if __name__ == "__main__":
    main()
