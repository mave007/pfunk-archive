#!/usr/bin/env python3
"""Deterministic normalization, lineage enrichment, and dedupe."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import (  # noqa: E402
    DISCOGRAPHY_COLUMNS as EXPECTED_COLUMNS,
    LINEAGE_COLUMNS,
    slug_hash,
    clean_title,
    base_work_title,
    infer_version_type,
    dedupe_key,
    safe_write_csv,
    validate_csv_input,
)


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"


def split_csv_list(value: str) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def join_csv_list(values: list[str]) -> str:
    seen = set()
    ordered = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            ordered.append(value)
    return ", ".join(ordered)


def normalize_date(value: str) -> str:
    return (value or "").strip()




def normalize_duration_seconds(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if re.fullmatch(r"\d+", value):
        return str(int(value))
    return ""


def release_id_for_row(row: dict[str, str]) -> str:
    artist_id = slug_hash("art", row.get("artist", ""))
    return slug_hash(
        "rel",
        artist_id,
        row.get("album_name", ""),
        row.get("release_date", ""),
        row.get("release_category", ""),
        row.get("edition_type", ""),
    )


def album_track_totals(rows: list[dict[str, str]]) -> dict[tuple[str, str], int]:
    totals: dict[tuple[str, str], int] = {}
    max_track_num: dict[tuple[str, str], int] = defaultdict(int)

    for row in rows:
        if row.get("row_type") != "track":
            continue
        key = (row.get("artist", "").strip(), row.get("album_name", "").strip())
        track_position = row.get("track_position", "").strip()
        if not track_position:
            continue
        full = re.fullmatch(r"(\d+)/(\d+)", track_position)
        if full:
            max_track_num[key] = max(max_track_num[key], int(full.group(2)))
            continue
        bare = re.fullmatch(r"\d+", track_position)
        if bare:
            max_track_num[key] = max(max_track_num[key], int(bare.group(0)))

    totals.update(max_track_num)
    return totals


def normalize_track_position(row: dict[str, str], totals: dict[tuple[str, str], int]) -> str:
    raw = row.get("track_position", "").strip()
    if row.get("row_type") != "track":
        return ""
    if not raw:
        return raw
    if re.fullmatch(r"\d+/\d+", raw):
        return raw
    if re.fullmatch(r"\d+", raw):
        key = (row.get("artist", "").strip(), row.get("album_name", "").strip())
        total = totals.get(key)
        if total and total >= int(raw):
            return f"{int(raw)}/{total}"
    return raw




def merge_rows(base: dict[str, str], incoming: dict[str, str]) -> dict[str, str]:
    merged = dict(base)

    if not merged.get("spotify_url", "").strip():
        merged["spotify_url"] = incoming.get("spotify_url", "").strip()
    if not merged.get("youtube_url", "").strip():
        merged["youtube_url"] = incoming.get("youtube_url", "").strip()

    merged["alternative_names"] = join_csv_list(
        split_csv_list(merged.get("alternative_names", ""))
        + split_csv_list(incoming.get("alternative_names", ""))
    )

    if not merged.get("chart_position", "").strip():
        merged["chart_position"] = incoming.get("chart_position", "").strip()
    if not merged.get("awards", "").strip():
        merged["awards"] = incoming.get("awards", "").strip()
    for field in LINEAGE_COLUMNS:
        if not merged.get(field, "").strip():
            merged[field] = incoming.get(field, "").strip()

    notes = [item.strip() for item in [merged.get("notes", ""), incoming.get("notes", "")] if item.strip()]
    if notes:
        seen = set()
        ordered = []
        for note in notes:
            if note not in seen:
                seen.add(note)
                ordered.append(note)
        merged["notes"] = " | ".join(ordered)

    return merged


def apply_lineage(rows: list[dict[str, str]]) -> int:
    source_candidates: dict[str, list[tuple[str, str]]] = defaultdict(list)
    source_candidates_by_artist: dict[str, list[tuple[str, str]]] = defaultdict(list)
    update_count = 0

    # First pass: compute base lineage fields and source candidate pool.
    for row in rows:
        for col in LINEAGE_COLUMNS:
            row.setdefault(col, "")

        work_title = base_work_title(row.get("song_name", ""))
        artist_id = slug_hash("art", row.get("artist", ""))
        work_id = row.get("work_id", "").strip() or slug_hash("wrk", artist_id, work_title.lower())
        version_source = clean_title(row.get("song_name", ""))
        version_id = row.get("version_id", "").strip() or slug_hash(
            "ver",
            work_id,
            version_source.lower(),
            row.get("duration_seconds", ""),
        )
        version_type = infer_version_type(
            row.get("song_name", ""),
            row.get("notes", ""),
            row.get("version_type", "").strip(),
        )
        duration_seconds = normalize_duration_seconds(row.get("duration_seconds", ""))
        release_id = release_id_for_row(row)

        before = tuple(row.get(col, "") for col in LINEAGE_COLUMNS)

        row["work_id"] = work_id
        row["version_id"] = version_id
        row["version_type"] = version_type
        row["duration_seconds"] = duration_seconds

        if row.get("release_category") != "compilation":
            source_candidates[work_id].append((row.get("release_date", ""), release_id))
            source_candidates_by_artist[row.get("artist", "").strip().lower()].append((row.get("release_date", ""), release_id))

        after = tuple(row.get(col, "") for col in LINEAGE_COLUMNS)
        if before != after:
            update_count += 1

    # Pick deterministic earliest source release.
    best_source: dict[str, str] = {}
    for work_id, candidates in source_candidates.items():
        ordered = sorted(candidates, key=lambda item: item[0] or "9999")
        best_source[work_id] = ordered[0][1]
    best_artist_source: dict[str, str] = {}
    for artist, candidates in source_candidates_by_artist.items():
        ordered = sorted(candidates, key=lambda item: item[0] or "9999")
        best_artist_source[artist] = ordered[0][1]

    # Second pass: assign source_release_id for compilation tracks.
    def is_compilation_track(r: dict[str, str]) -> bool:
        return r.get("row_type") == "track" and r.get("release_category") == "compilation"

    for row in rows:
        if is_compilation_track(row):
            source = row.get("source_release_id", "").strip() or best_source.get(row.get("work_id", ""), "")
            if not source:
                source = best_artist_source.get(row.get("artist", "").strip().lower(), "")
            before_source = row.get("source_release_id", "")
            row["source_release_id"] = source
            if row["source_release_id"] != before_source:
                update_count += 1

    return update_count


def normalize_and_dedupe(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], dict[str, int]]:
    totals = album_track_totals(rows)
    normalized_rows = []
    normalized_count = 0

    for row in rows:
        row_copy = dict(row)
        for field in LINEAGE_COLUMNS:
            row_copy.setdefault(field, "")
        row_copy["release_date"] = normalize_date(row_copy.get("release_date", ""))
        row_copy["track_position"] = normalize_track_position(row_copy, totals)
        normalized_rows.append(row_copy)
        if row_copy["track_position"] != row.get("track_position", "").strip():
            normalized_count += 1

    deduped: list[dict[str, str]] = []
    index_by_key: dict[tuple[str, ...], int] = {}
    duplicate_row_count = 0

    for row in normalized_rows:
        key = dedupe_key(row)
        if key in index_by_key:
            duplicate_row_count += 1
            idx = index_by_key[key]
            deduped[idx] = merge_rows(deduped[idx], row)
        else:
            index_by_key[key] = len(deduped)
            deduped.append(row)

    lineage_updated_count = apply_lineage(deduped)

    return deduped, {
        "track_position_normalized_count": normalized_count,
        "duplicate_row_count_removed": duplicate_row_count,
        "lineage_fields_updated_count": lineage_updated_count,
        "rows_before": len(rows),
        "rows_after": len(deduped),
    }


def ensure_columns(fieldnames: list[str]) -> list[str]:
    if not fieldnames:
        return EXPECTED_COLUMNS
    ordered = list(fieldnames)
    for col in EXPECTED_COLUMNS:
        if col not in ordered:
            ordered.append(col)
    return ordered




def write_report(report_json: Path, report_md: Path, metrics: dict[str, int]) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_md.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
    }
    report_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [
        "# Canonicalization Report",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- rows_before: {metrics['rows_before']}",
        f"- rows_after: {metrics['rows_after']}",
        f"- duplicate_row_count_removed: {metrics['duplicate_row_count_removed']}",
        f"- track_position_normalized_count: {metrics['track_position_normalized_count']}",
        f"- lineage_fields_updated_count: {metrics['lineage_fields_updated_count']}",
        "",
    ]
    report_md.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Canonicalize and dedupe discography.csv")
    parser.add_argument("--write", action="store_true", help="Write changes back to CSV")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--report-md", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows, raw_fieldnames = validate_csv_input(CSV_PATH, min_rows=1)
    fieldnames = ensure_columns(raw_fieldnames)

    updated_rows, metrics = normalize_and_dedupe(rows)
    if args.write:
        safe_write_csv(CSV_PATH, updated_rows, fieldnames, expected_columns=EXPECTED_COLUMNS)
    write_report(args.report_json, args.report_md, metrics)

    print(f"rows_before={metrics['rows_before']}")
    print(f"rows_after={metrics['rows_after']}")
    print(f"duplicates_removed={metrics['duplicate_row_count_removed']}")
    print(f"track_position_normalized={metrics['track_position_normalized_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
