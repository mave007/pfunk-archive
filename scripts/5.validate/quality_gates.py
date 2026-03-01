#!/usr/bin/env python3
"""Run deterministic quality gates and write daily-ready reports."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import DISCOGRAPHY_COLUMNS as EXPECTED_COLUMNS, dedupe_key  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
DISCOGRAPHY = ROOT / "data" / "discography.csv"
TRACKING = ROOT / "data" / "url_search_log.json"
CATALOG_ARTISTS = ROOT / "data" / "catalog_artists.csv"
CATALOG_RELEASES = ROOT / "data" / "catalog_releases.csv"
CATALOG_WORKS = ROOT / "data" / "catalog_works.csv"
CATALOG_TRACKS = ROOT / "data" / "catalog_tracks.csv"
CATALOG_PERSONNEL = ROOT / "data" / "catalog_personnel.csv"
DUPLICATES_REPORT = ROOT / "reports" / "duplicates" / "song_name_fuzzy_latest.json"
DUPLICATES_HIGH_CONF_REPORT = ROOT / "reports" / "duplicates" / "high_confidence_candidates.json"


def load_csv(path: Path) -> tuple[list[dict[str, str | None]], list[str]]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader), reader.fieldnames or []


def field(row: dict[str, str | None], key: str) -> str:
    return (row.get(key) or "").strip()


def field_lower(row: dict[str, str | None], key: str) -> str:
    return field(row, key).lower()




def run_gates() -> dict:
    issues: list[str] = []
    warnings: list[str] = []

    rows, fieldnames = load_csv(DISCOGRAPHY)
    if fieldnames != EXPECTED_COLUMNS:
        issues.append("Schema header mismatch in data/discography.csv")

    duplicate_counter = Counter(dedupe_key(row) for row in rows)
    duplicate_count = sum(value - 1 for value in duplicate_counter.values() if value > 1)
    if duplicate_count > 0:
        issues.append(f"Duplicate rows detected: {duplicate_count}")

    invalid_track_position = 0
    bare_digit_track_position = 0
    invalid_release_date = 0
    for row in rows:
        track_position = field(row, "track_position")
        row_type = field(row, "row_type")
        if row_type == "track":
            if not track_position:
                invalid_track_position += 1
            elif re.fullmatch(r"\d+/\d+", track_position):
                pass
            elif re.fullmatch(r"\d+", track_position):
                bare_digit_track_position += 1
            else:
                invalid_track_position += 1
        elif track_position:
            warnings.append("Non-track row has track_position")
        release_date = field(row, "release_date")
        if release_date and not re.fullmatch(r"\d{4}(-\d{2})?(-\d{2})?", release_date):
            invalid_release_date += 1
    if invalid_track_position:
        issues.append(f"Invalid track_position rows: {invalid_track_position}")
    if bare_digit_track_position:
        warnings.append(f"Track rows with bare digit track_position (no total): {bare_digit_track_position}")
    if invalid_release_date:
        issues.append(f"Invalid release_date rows: {invalid_release_date}")

    if TRACKING.exists():
        tracking_payload = json.loads(TRACKING.read_text(encoding="utf-8"))
        entries = tracking_payload.get("entries", [])
        if len(entries) != len(rows):
            issues.append(f"Tracking entry mismatch: rows={len(rows)}, entries={len(entries)}")
        row_numbers = [entry.get("row_number") for entry in entries if isinstance(entry.get("row_number"), int)]
        if len(set(row_numbers)) != len(rows):
            issues.append("Tracking row_number uniqueness mismatch")
    else:
        entries = []
        warnings.append("Tracking file url_search_log.json not found -- skipping tracking checks")

    catalog_files_present = all(p.exists() for p in [CATALOG_ARTISTS, CATALOG_RELEASES, CATALOG_WORKS, CATALOG_TRACKS])
    if catalog_files_present:
        artists, _ = load_csv(CATALOG_ARTISTS)
        releases, _ = load_csv(CATALOG_RELEASES)
        works, _ = load_csv(CATALOG_WORKS)
        tracks, _ = load_csv(CATALOG_TRACKS)

        artist_ids = {row["artist_id"] for row in artists}
        release_ids = {row["release_id"] for row in releases}
        work_ids = {row["work_id"] for row in works}

        missing_artist_fk = 0
        missing_release_fk = 0
        missing_work_fk = 0
        for row in tracks:
            if row.get("artist_id") not in artist_ids:
                missing_artist_fk += 1
            if row.get("release_id") not in release_ids:
                missing_release_fk += 1
            if row.get("work_id") not in work_ids:
                missing_work_fk += 1

        if missing_artist_fk:
            issues.append(f"Catalog track rows missing artist FK: {missing_artist_fk}")
        if missing_release_fk:
            issues.append(f"Catalog track rows missing release FK: {missing_release_fk}")
        if missing_work_fk:
            issues.append(f"Catalog track rows missing work FK: {missing_work_fk}")

        if CATALOG_PERSONNEL.exists():
            personnel, _ = load_csv(CATALOG_PERSONNEL)
            orphan_personnel = sum(1 for r in personnel if r.get("release_id") not in release_ids)
            if orphan_personnel:
                warnings.append(f"Personnel rows with no matching release: {orphan_personnel}")
    else:
        tracks = []
        warnings.append("Catalog files not found -- skipping FK checks")

    duplicate_high_conf_clusters = 0
    duplicate_high_conf_pairs = 0
    duplicate_clusters_missing_original = 0
    duplicate_same_master_conflict_clusters = 0
    duplicate_probable_cover_pairs = 0

    if DUPLICATES_HIGH_CONF_REPORT.exists():
        try:
            high_conf_payload = json.loads(DUPLICATES_HIGH_CONF_REPORT.read_text(encoding="utf-8"))
            high_conf_clusters = high_conf_payload.get("clusters", [])
            high_conf_pairs = high_conf_payload.get("pairs", [])
            duplicate_high_conf_clusters = len(high_conf_clusters)
            duplicate_high_conf_pairs = len(high_conf_pairs)
            duplicate_clusters_missing_original = sum(
                1 for cluster in high_conf_clusters if not cluster.get("original_candidate")
            )
            for cluster in high_conf_clusters:
                members = cluster.get("members", [])
                version_types = {
                    str(member.get("version_type", "") or "").strip().lower()
                    for member in members
                    if member.get("version_type")
                }
                if "same_master" in version_types and any(item != "same_master" for item in version_types):
                    duplicate_same_master_conflict_clusters += 1
            if duplicate_clusters_missing_original:
                warnings.append(
                    f"Duplicate high-confidence clusters missing original candidate: {duplicate_clusters_missing_original}"
                )
            if duplicate_same_master_conflict_clusters:
                warnings.append(
                    "Duplicate high-confidence clusters contain mixed same_master and variant version_type values: "
                    f"{duplicate_same_master_conflict_clusters}"
                )
        except json.JSONDecodeError:
            warnings.append("Duplicate high-confidence report is not valid JSON")
    else:
        warnings.append("Duplicate high-confidence report not found")

    if DUPLICATES_REPORT.exists():
        try:
            full_duplicate_payload = json.loads(DUPLICATES_REPORT.read_text(encoding="utf-8"))
            relation_counts = (
                full_duplicate_payload.get("metrics", {})
                .get("match_type_counts", {})
                .get("relation_class", {})
            )
            duplicate_probable_cover_pairs = int(relation_counts.get("probable_cover_or_interpolation", 0))
            if duplicate_probable_cover_pairs:
                warnings.append(
                    "Duplicate detector cross-artist probable cover/interpolation pairs: "
                    f"{duplicate_probable_cover_pairs}"
                )
        except json.JSONDecodeError:
            warnings.append("Duplicate report is not valid JSON")
    else:
        warnings.append("Duplicate report not found")

    status = "PASS" if not issues else "FAIL"
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "metrics": {
            "rows": len(rows),
            "tracking_entries": len(entries),
            "duplicate_rows": duplicate_count,
            "invalid_track_position_rows": invalid_track_position,
            "invalid_release_date_rows": invalid_release_date,
            "catalog_tracks": len(tracks),
            "duplicate_high_conf_cluster_count": duplicate_high_conf_clusters,
            "duplicate_high_conf_pair_count": duplicate_high_conf_pairs,
            "duplicate_clusters_missing_original_count": duplicate_clusters_missing_original,
            "duplicate_same_master_conflict_cluster_count": duplicate_same_master_conflict_clusters,
            "duplicate_probable_cover_pair_count": duplicate_probable_cover_pairs,
        },
        "issues": issues,
        "warnings": sorted(set(warnings)),
    }


def write_reports(result: dict, report_json: Path, report_md: Path) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(result, indent=2), encoding="utf-8")

    lines = [
        "# Quality Gates Report",
        "",
        f"- generated_at: {result['generated_at']}",
        f"- status: {result['status']}",
        "",
        "## Metrics",
        "",
    ]
    for key, value in result["metrics"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Issues", ""])
    if result["issues"]:
        for issue in result["issues"]:
            lines.append(f"- {issue}")
    else:
        lines.append("- none")
    lines.extend(["", "## Warnings", ""])
    if result["warnings"]:
        for warning in result["warnings"]:
            lines.append(f"- {warning}")
    else:
        lines.append("- none")
    report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run quality gates")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--report-md", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_gates()
    write_reports(result, args.report_json, args.report_md)
    print(f"status={result['status']}")
    print(f"issues={len(result['issues'])}")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
