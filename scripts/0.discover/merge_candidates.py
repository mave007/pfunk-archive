#!/usr/bin/env python3
"""Auto-merge high-confidence discovery candidates into discography.csv and queue the rest for manual review.

Idempotency: candidates are checked against discography via fuzzy matching
before appending.  A pre-merge schema gate rejects malformed rows.
"""

import argparse
import csv
import os
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import (  # noqa: E402
    DISCOGRAPHY_COLUMNS,
    CANDIDATE_COLUMNS,
    VALID_ROW_TYPES,
    normalize_for_matching,
    safe_write_csv,
)

ROOT = Path(__file__).resolve().parent.parent.parent

CANDIDATES_PATH = ROOT / "data" / "discovery_candidates.csv"
DISCOGRAPHY_PATH = ROOT / "data" / "discography.csv"
REVIEW_QUEUE_PATH = ROOT / "reports" / "discovery" / "review_queue.csv"
REJECTED_PATH = ROOT / "reports" / "discovery" / "rejected.csv"

CONFIDENCE_ORDER = ["low", "medium", "high"]
FUZZY_RATIO_THRESHOLD = 0.85
DATE_RE = re.compile(r"^\d{4}(-\d{2})?(-\d{2})?$")
MAX_FIELD_LEN = 500


def meets_confidence_threshold(overall_confidence: str, threshold: str) -> bool:
    try:
        return CONFIDENCE_ORDER.index(overall_confidence.lower()) >= CONFIDENCE_ORDER.index(
            threshold.lower()
        )
    except (ValueError, AttributeError):
        return False


def validate_candidate(row: dict) -> str | None:
    """Return a rejection reason string, or None if the candidate is valid."""
    artist = (row.get("artist") or "").strip()
    if not artist:
        return "empty artist"

    song = (row.get("song_name") or "").strip()
    album = (row.get("album_name") or "").strip()
    if not song and not album:
        return "both song_name and album_name are empty"

    row_type = (row.get("row_type") or "").strip()
    if row_type and row_type not in VALID_ROW_TYPES:
        return f"invalid row_type '{row_type}'"

    release_date = (row.get("release_date") or "").strip()
    if release_date and not DATE_RE.fullmatch(release_date):
        return f"invalid release_date format '{release_date}'"

    for field_name in ("artist", "song_name", "album_name", "label"):
        val = (row.get(field_name) or "").strip()
        if len(val) > MAX_FIELD_LEN:
            return f"{field_name} exceeds {MAX_FIELD_LEN} chars"

    return None


def build_fuzzy_lookup(rows: list[dict]) -> list[str]:
    """Build normalized signatures for fuzzy matching."""
    sigs = []
    for r in rows:
        parts = [
            normalize_for_matching(r.get("artist", "")),
            normalize_for_matching(r.get("album_name", "")),
            normalize_for_matching(r.get("song_name", "")),
        ]
        sigs.append("".join(parts))
    return sigs


def fuzzy_matches_existing(candidate: dict, existing_sigs: list[str]) -> bool:
    """Check if candidate fuzzy-matches any existing discography row."""
    c_sig = "".join([
        normalize_for_matching(candidate.get("artist", "")),
        normalize_for_matching(candidate.get("album_name", "")),
        normalize_for_matching(candidate.get("song_name", "")),
    ])
    for sig in existing_sigs:
        if SequenceMatcher(None, c_sig, sig).ratio() >= FUZZY_RATIO_THRESHOLD:
            return True
    return False


def candidate_to_discography_row(row: dict) -> dict:
    sources = row.get("sources", "")
    notes = f"discovered via {sources}" if sources else "discovered (no sources)"
    return {col: "" for col in DISCOGRAPHY_COLUMNS} | {
        "artist": row.get("artist", ""),
        "song_name": row.get("song_name", ""),
        "album_name": row.get("album_name", ""),
        "row_type": row.get("row_type", ""),
        "release_date": row.get("release_date", ""),
        "label": row.get("label", ""),
        "notes": notes,
    }


def load_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))




def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge high-confidence discovery candidates into discography"
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Actually modify discography.csv and update candidates (default: dry-run)",
    )
    parser.add_argument(
        "--confidence-threshold",
        type=str,
        default="high",
        help="Minimum confidence to auto-merge (default: high)",
    )
    args = parser.parse_args()

    if not CANDIDATES_PATH.exists():
        print(f"Error: {CANDIDATES_PATH} not found")
        return

    all_candidates = load_csv_rows(CANDIDATES_PATH)
    pending = [r for r in all_candidates if (r.get("status") or "").strip().lower() == "pending"]
    skipped = len(all_candidates) - len(pending)

    above_threshold = [
        r for r in pending
        if meets_confidence_threshold(r.get("overall_confidence", "low"), args.confidence_threshold)
    ]

    existing_rows = load_csv_rows(DISCOGRAPHY_PATH)
    existing_sigs = build_fuzzy_lookup(existing_rows)
    existing_fieldnames = (
        list(existing_rows[0].keys()) if existing_rows else list(DISCOGRAPHY_COLUMNS)
    )

    auto_merge: list[dict] = []
    fuzzy_dupes: list[dict] = []
    rejected: list[dict] = []

    for row in above_threshold:
        reason = validate_candidate(row)
        if reason:
            row["_rejection_reason"] = reason
            rejected.append(row)
            continue
        if fuzzy_matches_existing(row, existing_sigs):
            row["_rejection_reason"] = "fuzzy duplicate of existing discography row"
            fuzzy_dupes.append(row)
            continue
        auto_merge.append(row)

    review_queue = [r for r in pending if r not in above_threshold] + fuzzy_dupes

    candidate_fieldnames = list(all_candidates[0].keys()) if all_candidates else list(CANDIDATE_COLUMNS)

    if args.write:
        print("--write: applying changes to discography.csv and discovery_candidates.csv")
        if auto_merge:
            new_rows = [candidate_to_discography_row(r) for r in auto_merge]
            merged_rows = existing_rows + new_rows
            safe_write_csv(DISCOGRAPHY_PATH, merged_rows, existing_fieldnames)

            for row in auto_merge:
                row["status"] = "merged"
            for row in fuzzy_dupes:
                row["status"] = "fuzzy_duplicate"
            for row in rejected:
                row["status"] = "rejected"
            safe_write_csv(CANDIDATES_PATH, all_candidates, candidate_fieldnames, backup=False)
    else:
        print("Dry-run (use --write to apply changes)")

    safe_write_csv(REVIEW_QUEUE_PATH, review_queue, candidate_fieldnames, backup=False)

    if rejected:
        rejected_fieldnames = candidate_fieldnames + ["_rejection_reason"]
        safe_write_csv(REJECTED_PATH, rejected, rejected_fieldnames, backup=False)

    print(f"Auto-merged: {len(auto_merge)}")
    print(f"Fuzzy duplicates (sent to review): {len(fuzzy_dupes)}")
    print(f"Rejected (schema failures): {len(rejected)}")
    print(f"Queued for review: {len(review_queue)}")
    print(f"Skipped (existing/merged): {skipped}")


if __name__ == "__main__":
    main()
