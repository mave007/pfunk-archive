#!/usr/bin/env python3
"""Score Spotify URL mismatches against discography metadata."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import (  # noqa: E402
    DISCOGRAPHY_COLUMNS, ProgressTracker,
    load_env, require_env, validate_csv_input,
)

load_env()

ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
CACHE_DIR = ROOT / "data" / ".spotify_cache"
TRACK_META_CACHE = CACHE_DIR / "url_meta_tracks.json"
ALBUM_META_CACHE = CACHE_DIR / "url_meta_albums.json"


def norm(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"^\[[^\]]+\]\s*", "", text)
    text = re.sub(r"\((feat\.|ft\.|featuring)\s+[^)]*\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(feat\.?|ft\.?|featuring)\b.*$", "", text, flags=re.IGNORECASE)
    text = text.replace("&", " and ")
    text = re.sub(r"\b4\b", "for", text)
    text = re.sub(r"\b2\b", "to", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def ratio(left: str, right: str) -> float:
    return SequenceMatcher(None, norm(left), norm(right)).ratio()


VARIANT_TERM_RE = re.compile(
    r"\b(?:mono|stereo|version|mix|remix|remaster(?:ed)?|live|instrumental|instr|"
    r"edit|extended|long|short|radio|dub|rework(?:ed)?|flip|demo|alternate|alt|"
    r"single|album|unedited|bonus|track|pt|part)\b",
    flags=re.IGNORECASE,
)


def strip_variant_terms(value: str) -> str:
    text = VARIANT_TERM_RE.sub(" ", value)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def title_forms(value: str) -> list[str]:
    raw = (value or "").strip()
    if not raw:
        return []

    forms: set[str] = set()

    def add_form(candidate: str) -> None:
        normalized = norm(candidate)
        if not normalized:
            return
        forms.add(normalized)
        stripped = strip_variant_terms(normalized)
        if stripped:
            forms.add(stripped)

    add_form(raw)
    add_form(re.sub(r"\([^)]*\)", "", raw))
    for part in re.split(r"\s*/\s*|\s*;\s*", raw):
        add_form(part)

    # Handle alias titles such as "a.k.a. Foo - Bar" by indexing alias forms.
    aka_match = re.search(r"\ba\.?k\.?a\.?\b(.+)$", raw, flags=re.IGNORECASE)
    if aka_match:
        alias_tail = aka_match.group(1).strip(" -:;")
        add_form(alias_tail)
        for part in re.split(r"\s+-\s+", alias_tail):
            add_form(part)

    return sorted(forms)


def token_jaccard(left: str, right: str) -> float:
    left_set = {token for token in left.split() if token}
    right_set = {token for token in right.split() if token}
    if not left_set or not right_set:
        return 0.0
    union = left_set | right_set
    if not union:
        return 0.0
    return len(left_set & right_set) / len(union)


def title_similarity(expected: str, spotify_title: str) -> float:
    expected_forms = title_forms(expected)
    spotify_forms = title_forms(spotify_title)
    if not expected_forms or not spotify_forms:
        return 0.0

    best = 0.0
    for left in expected_forms:
        for right in spotify_forms:
            seq_score = SequenceMatcher(None, left, right).ratio()
            token_score = token_jaccard(left, right)
            score = max(seq_score, token_score)
            best = max(best, score)
    return best


def split_artist_credit(artist: str) -> list[str]:
    raw = (artist or "").strip()
    if not raw:
        return []
    parts = re.split(
        r"\s*(?:,|/|&| and | with | feat\.?|ft\.?|featuring)\s*",
        raw,
        flags=re.IGNORECASE,
    )
    cleaned = [part.strip() for part in parts if part.strip()]
    # Keep full credit for fallback plus split components for robust matching.
    candidates = [raw] + cleaned
    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = norm(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def parse_spotify_url(url: str) -> tuple[str, str] | None:
    match = re.search(r"open\.spotify\.com/(track|album)/([A-Za-z0-9]+)", url)
    if not match:
        return None
    return match.group(1), match.group(2)


def expected_url_type(row_type: str) -> str:
    return "album" if row_type == "album" else "track"


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[idx : idx + size] for idx in range(0, len(values), size)]


def fetch_missing_track_meta(
    spotify: spotipy.Spotify,
    ids: list[str],
    cache_payload: dict[str, Any],
) -> None:
    missing = [item for item in ids if item not in cache_payload]
    for group in chunked(missing, 50):
        response = spotify.tracks(group)
        for track in response.get("tracks", []):
            if not track:
                continue
            track_id = track.get("id", "")
            if not track_id:
                continue
            artists = [
                artist.get("name", "").strip()
                for artist in (track.get("artists") or [])
                if isinstance(artist, dict)
            ]
            cache_payload[track_id] = {
                "name": track.get("name", "").strip(),
                "artists": artists,
                "album_name": (track.get("album") or {}).get("name", "").strip(),
            }


def fetch_missing_album_meta(
    spotify: spotipy.Spotify,
    ids: list[str],
    cache_payload: dict[str, Any],
) -> None:
    missing = [item for item in ids if item not in cache_payload]
    for group in chunked(missing, 20):
        response = spotify.albums(group)
        for album in response.get("albums", []):
            if not album:
                continue
            album_id = album.get("id", "")
            if not album_id:
                continue
            artists = [
                artist.get("name", "").strip()
                for artist in (album.get("artists") or [])
                if isinstance(artist, dict)
            ]
            cache_payload[album_id] = {
                "name": album.get("name", "").strip(),
                "artists": artists,
            }


def quarantine_recommended(
    *,
    type_mismatch: bool,
    title_score: float,
    artist_score: float,
) -> bool:
    if type_mismatch:
        return True
    if title_score < 0.35:
        return True
    if artist_score < 0.25:
        return True
    if title_score < 0.5 and artist_score < 0.35:
        return True
    return False


def classify_severity(
    *,
    type_mismatch: bool,
    title_score: float,
    artist_score: float,
) -> str:
    if type_mismatch or title_score < 0.35 or artist_score < 0.25:
        return "high"
    if title_score < 0.55 or artist_score < 0.4:
        return "medium"
    return "low"


def build_markdown(
    report_md: Path,
    payload: dict[str, Any],
) -> None:
    findings = payload.get("findings", [])
    lines = [
        "# Spotify Link Mismatch Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- rows_with_spotify_url: {payload['metrics']['rows_with_spotify_url']}",
        f"- audited_rows: {payload['metrics']['audited_rows']}",
        f"- metadata_missing_rows: {payload['metrics']['metadata_missing_rows']}",
        f"- mismatched_rows: {payload['metrics']['mismatched_rows']}",
        f"- quarantine_recommended_rows: {payload['metrics']['quarantine_recommended_rows']}",
        "",
        "## Severity Counts",
        "",
    ]
    for severity, count in payload.get("severity_counts", {}).items():
        lines.append(f"- {severity}: {count}")
    lines.extend(
        [
            "",
            "## Top Mismatch Findings",
            "",
            "| row_number | severity | row_type | artist | expected_title | spotify_title | title_score | artist_score | type_mismatch |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for item in findings[:200]:
        lines.append(
            f"| {item['row_number']} | {item['severity']} | {item['row_type']} | "
            f"{item['artist']} | {item['expected_title']} | {item['spotify_title']} | "
            f"{item['title_score']:.3f} | {item['artist_score']:.3f} | {str(item['type_mismatch']).lower()} |"
        )
    if not findings:
        lines.append("| - | - | - | - | - | - | - | - | - |")
    lines.append("")
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score Spotify URL mismatches")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--report-md", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows, _ = validate_csv_input(CSV_PATH, DISCOGRAPHY_COLUMNS, min_rows=1)

    links: list[tuple[int, dict[str, str], str, str]] = []
    track_ids: set[str] = set()
    album_ids: set[str] = set()
    for idx, row in enumerate(rows, start=2):
        url = (row.get("spotify_url") or "").strip()
        if not url:
            continue
        parsed = parse_spotify_url(url)
        if not parsed:
            continue
        url_type, entity_id = parsed
        links.append((idx, row, url_type, entity_id))
        if url_type == "track":
            track_ids.add(entity_id)
        else:
            album_ids.add(entity_id)

    client_id = require_env("SPOTIPY_CLIENT_ID")
    client_secret = require_env("SPOTIPY_CLIENT_SECRET")
    spotify = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    )

    track_meta = load_json(TRACK_META_CACHE)
    album_meta = load_json(ALBUM_META_CACHE)
    fetch_missing_track_meta(spotify, sorted(track_ids), track_meta)
    fetch_missing_album_meta(spotify, sorted(album_ids), album_meta)
    save_json(TRACK_META_CACHE, track_meta)
    save_json(ALBUM_META_CACHE, album_meta)

    findings: list[dict[str, Any]] = []
    audited_rows = 0
    metadata_missing_rows = 0
    progress = ProgressTracker(total=len(links), noun="links")
    for row_number, row, url_type, entity_id in links:
        meta = track_meta.get(entity_id) if url_type == "track" else album_meta.get(entity_id)
        if not meta:
            metadata_missing_rows += 1
            continue
        audited_rows += 1
        row_type = (row.get("row_type") or "").strip()
        expected_type = expected_url_type(row_type)
        type_mismatch = url_type != expected_type
        expected_title = (row.get("album_name") or "").strip() if row_type == "album" else (row.get("song_name") or "").strip()
        spotify_title = (meta.get("name") or "").strip()
        title_score = title_similarity(expected_title, spotify_title)
        artist = (row.get("artist") or "").strip()
        spotify_artists = [item for item in (meta.get("artists") or []) if item]
        artist_score = 0.0
        if spotify_artists:
            expected_artists = split_artist_credit(artist)
            pair_scores = [
                ratio(expected, candidate)
                for expected in expected_artists
                for candidate in spotify_artists
            ]
            artist_score = max(pair_scores, default=0.0)
            # Compilation rows often use "Various Artists" while Spotify stores a primary credit.
            if norm(artist) == "various artists":
                artist_score = max(artist_score, 0.75)
        suspicious = type_mismatch or title_score < 0.55 or artist_score < 0.4
        progress.update(extra=f"| audited={audited_rows} mismatches={len(findings)}")
        if not suspicious:
            continue
        severity = classify_severity(type_mismatch=type_mismatch, title_score=title_score, artist_score=artist_score)
        reasons = []
        if type_mismatch:
            reasons.append("url_type_mismatch")
        if title_score < 0.55:
            reasons.append("low_title_similarity")
        if artist_score < 0.4:
            reasons.append("low_artist_similarity")
        findings.append(
            {
                "row_number": row_number,
                "row_type": row_type,
                "artist": artist,
                "album_name": row.get("album_name", ""),
                "song_name": row.get("song_name", ""),
                "spotify_url": row.get("spotify_url", ""),
                "expected_type": expected_type,
                "spotify_type": url_type,
                "type_mismatch": type_mismatch,
                "expected_title": expected_title,
                "spotify_title": spotify_title,
                "spotify_artists": spotify_artists,
                "title_score": round(title_score, 4),
                "artist_score": round(artist_score, 4),
                "severity": severity,
                "reason_codes": reasons,
                "quarantine_recommended": quarantine_recommended(
                    type_mismatch=type_mismatch,
                    title_score=title_score,
                    artist_score=artist_score,
                ),
            }
        )

    progress.finish(extra=f"| {len(findings)} mismatches found")

    findings.sort(key=lambda item: (item["severity"], item["title_score"], item["artist_score"], item["row_number"]))
    severity_counts = {"high": 0, "medium": 0, "low": 0}
    quarantine_count = 0
    for item in findings:
        severity_counts[item["severity"]] += 1
        if item["quarantine_recommended"]:
            quarantine_count += 1

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "rows_with_spotify_url": len(links),
            "audited_rows": audited_rows,
            "metadata_missing_rows": metadata_missing_rows,
            "mismatched_rows": len(findings),
            "quarantine_recommended_rows": quarantine_count,
        },
        "severity_counts": severity_counts,
        "findings": findings,
    }
    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    build_markdown(args.report_md, payload)

    print(f"rows_with_spotify_url={len(links)}")
    print(f"audited_rows={audited_rows}")
    print(f"metadata_missing_rows={metadata_missing_rows}")
    print(f"mismatched_rows={len(findings)}")
    print(f"quarantine_recommended_rows={quarantine_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
