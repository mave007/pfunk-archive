#!/usr/bin/env python3
"""
Consolidate discovery source CSVs into deduplicated candidates.

Merges all CSV files from data/discovery_raw/, deduplicates with fuzzy matching,
scores confidence, diffs against discography.csv, and outputs consolidated
candidates plus a corrections queue for metadata divergences.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import (  # noqa: E402
    DISCOVERY_SOURCE_COLUMNS as SOURCE_COLUMNS,
    CANDIDATE_COLUMNS,
    normalize_for_matching,
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DISCOVERY_RAW_DIR = ROOT / "data" / "discovery_raw"
DISCOGRAPHY_PATH = ROOT / "data" / "discography.csv"
CANDIDATES_PATH = ROOT / "data" / "discovery_candidates.csv"
LOG_PATH = ROOT / "data" / "discovery_log.json"
CORRECTIONS_PATH = ROOT / "reports" / "discovery" / "corrections_queue.csv"

CORRECTION_COLUMNS = [
    "row_key",
    "field",
    "current_value",
    "proposed_value",
    "source",
    "confidence",
]

STRUCTURED_SOURCES = {"discogs", "spotify", "musicbrainz"}
FUZZY_RATIO_THRESHOLD = 0.85

SOURCE_WEIGHTS = {
    "musicbrainz": 0.9,
    "discogs": 0.9,
    "spotify": 0.7,
    "wikidata": 0.6,
    "wikipedia": 0.4,
    "georgeclinton": 0.3,
    "motherpage": 0.3,
    "forums": 0.2,
}


def fuzzy_match_key(a: str, b: str) -> bool:
    return SequenceMatcher(None, a, b).ratio() >= FUZZY_RATIO_THRESHOLD


def row_to_match_key(artist: str, album: str, song: str) -> str:
    return "|".join(
        normalize_for_matching(x) for x in [artist or "", album or "", song or ""]
    )


def build_match_key_sig(artist: str, album: str, song: str) -> str:
    return (
        normalize_for_matching(artist or "")
        + "||"
        + normalize_for_matching(album or "")
        + "||"
        + normalize_for_matching(song or "")
    )


def load_source_csvs() -> list[dict]:
    rows: list[dict] = []
    if not DISCOVERY_RAW_DIR.exists():
        return rows
    for p in sorted(DISCOVERY_RAW_DIR.glob("*.csv")):
        try:
            with p.open(encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    row = {k: (r.get(k) or "").strip() for k in SOURCE_COLUMNS}
                    if not row.get("discovery_source"):
                        row["discovery_source"] = p.stem
                    rows.append(row)
        except (csv.Error, OSError) as exc:
            logger.warning("Skipping %s: %s", p.name, exc)
    return rows


def load_discography() -> list[dict]:
    rows: list[dict] = []
    if not DISCOGRAPHY_PATH.exists():
        return rows
    with DISCOGRAPHY_PATH.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def deduplicate(rows: list[dict]) -> list[dict]:
    if not rows:
        return []
    groups: list[list[dict]] = []
    for r in rows:
        key_sig = build_match_key_sig(
            r.get("artist", ""),
            r.get("album_name", ""),
            r.get("song_name", ""),
        )
        merged = False
        for grp in groups:
            rep = grp[0]
            rep_sig = build_match_key_sig(
                rep.get("artist", ""),
                rep.get("album_name", ""),
                rep.get("song_name", ""),
            )
            comb_a = key_sig.replace("||", "")
            comb_b = rep_sig.replace("||", "")
            if fuzzy_match_key(comb_a, comb_b):
                grp.append(r)
                merged = True
                break
        if not merged:
            groups.append([r])
    result: list[dict] = []
    for grp in groups:
        best = pick_best_metadata(grp)
        sources = list({r.get("discovery_source", "") for r in grp if r.get("discovery_source")})
        best["_sources"] = sources
        best["_source_count"] = len(sources)
        best["_all_rows"] = grp
        result.append(best)
    return result


def pick_best_metadata(group: list[dict]) -> dict:
    base = group[0].copy()
    for r in group[1:]:
        for field in ["artist", "album_name", "song_name", "release_date", "label", "row_type"]:
            val = (r.get(field) or "").strip()
            if val and not (base.get(field) or "").strip():
                base[field] = val
        raw = (r.get("raw_extra") or "").strip()
        if raw and not (base.get("raw_extra") or "").strip():
            base["raw_extra"] = raw
    return base


def build_discography_lookup(discography: list[dict]) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for r in discography:
        artist = r.get("artist", "")
        album = r.get("album_name", "")
        song = r.get("song_name", "")
        key_sig = build_match_key_sig(artist, album, song)
        lookup[key_sig] = r
    return lookup


def find_discography_match(
    candidate: dict,
    discography_lookup: dict[str, dict],
) -> dict | None:
    ca = (candidate.get("artist") or "").strip()
    cb = (candidate.get("album_name") or "").strip()
    cc = (candidate.get("song_name") or "").strip()
    c_sig = build_match_key_sig(ca, cb, cc)
    return discography_lookup.get(c_sig)


def _compute_weighted_score(candidate: dict) -> float:
    """Compute a 0.0-1.0 confidence score using source weights and metadata signals."""
    sources = candidate.get("_sources", [])
    if not sources:
        return 0.0

    source_score = max(
        SOURCE_WEIGHTS.get(s.lower(), 0.1) for s in sources
    )

    completeness_bonus = 0.0
    for f in ["release_date", "label", "row_type"]:
        if (candidate.get(f) or "").strip():
            completeness_bonus += 0.1

    agreement_bonus = 0.0
    if len(sources) >= 2:
        structured = [s for s in sources if s.lower() in STRUCTURED_SOURCES]
        if len(structured) >= 2:
            all_rows = candidate.get("_all_rows", [])
            dates = [
                (r.get("release_date") or "").strip()[:4]
                for r in all_rows
                if (r.get("release_date") or "").strip()
                   and r.get("discovery_source", "").lower() in STRUCTURED_SOURCES
            ]
            if len(dates) >= 2 and len(set(dates)) == 1:
                agreement_bonus = 0.2

    raw = source_score + completeness_bonus + agreement_bonus
    return min(raw, 1.0)


def score_confidence(candidate: dict) -> str:
    score = _compute_weighted_score(candidate)
    candidate["_confidence_score"] = round(score, 2)
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"


def detect_corrections(
    candidate: dict,
    disc_row: dict,
    corrections: list[dict],
) -> None:
    row_key = row_to_match_key(
        disc_row.get("artist", ""),
        disc_row.get("album_name", ""),
        disc_row.get("song_name", ""),
    )
    fields_to_compare = ["artist", "album_name", "song_name", "release_date", "label", "row_type"]
    for field in fields_to_compare:
        curr = (disc_row.get(field) or "").strip()
        prop = (candidate.get(field) or "").strip()
        if prop and curr != prop:
            corrections.append(
                {
                    "row_key": row_key,
                    "field": field,
                    "current_value": curr,
                    "proposed_value": prop,
                    "source": (candidate.get("_sources") or ["unknown"])[0],
                    "confidence": candidate.get("overall_confidence", "low"),
                }
            )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raw_rows = load_source_csvs()
    discography = load_discography()
    discography_lookup = build_discography_lookup(discography)

    source_files = [
        p.name for p in (DISCOVERY_RAW_DIR.glob("*.csv") if DISCOVERY_RAW_DIR.exists() else [])
    ]

    deduped = deduplicate(raw_rows)
    corrections: list[dict] = []
    candidates: list[dict] = []

    for c in deduped:
        conf = score_confidence(c)
        c["overall_confidence"] = conf
        match = find_discography_match(c, discography_lookup)
        if match:
            c["status"] = "existing"
            detect_corrections(c, match, corrections)
        else:
            c["status"] = "pending"

        sources = c.get("_sources", [])
        out = {
            "artist": c.get("artist", ""),
            "album_name": c.get("album_name", ""),
            "song_name": c.get("song_name", ""),
            "release_date": c.get("release_date", ""),
            "label": c.get("label", ""),
            "row_type": c.get("row_type", ""),
            "overall_confidence": conf,
            "sources": ",".join(sorted(sources)),
            "source_count": len(sources),
            "status": c["status"],
            "raw_extra": c.get("raw_extra", ""),
        }
        candidates.append(out)

    CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    CORRECTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)

    with CANDIDATES_PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CANDIDATE_COLUMNS)
        w.writeheader()
        w.writerows(candidates)

    if corrections:
        with CORRECTIONS_PATH.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CORRECTION_COLUMNS)
            w.writeheader()
            w.writerows(corrections)

    log_data = {
        "timestamp": datetime.now().isoformat(),
        "source_files": source_files,
        "total_raw_rows": len(raw_rows),
        "unique_candidates": len(candidates),
        "pending": sum(1 for c in candidates if c["status"] == "pending"),
        "existing": sum(1 for c in candidates if c["status"] == "existing"),
        "corrections_found": len(corrections),
    }
    with LOG_PATH.open("w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)

    print("Summary:")
    print(f"  Total raw rows loaded: {len(raw_rows)}")
    print(f"  Unique candidates after dedup: {len(candidates)}")
    print(f"  New (pending): {sum(1 for c in candidates if c['status'] == 'pending')}")
    print(f"  Existing: {sum(1 for c in candidates if c['status'] == 'existing')}")
    print(f"  Corrections found: {len(corrections)}")


if __name__ == "__main__":
    main()
