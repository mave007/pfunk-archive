#!/usr/bin/env python3
"""Quarantine suspicious Spotify URLs into needs_research."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import safe_write_csv  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
TRACKING_PATH = ROOT / "data" / "url_search_log.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quarantine suspicious Spotify URLs")
    parser.add_argument("--audit-json", type=Path, required=True)
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--report-md", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    return parser.parse_args()


def load_csv() -> tuple[list[dict[str, str]], list[str]]:
    with CSV_PATH.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader), reader.fieldnames or []


def load_tracking() -> dict[str, Any]:
    if not TRACKING_PATH.exists():
        return {}
    return json.loads(TRACKING_PATH.read_text(encoding="utf-8"))


def build_markdown(report_md: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Spotify Link Quarantine Report",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- write_mode: {str(payload['write_mode']).lower()}",
        f"- rows_quarantined: {payload['rows_quarantined']}",
        "",
        "## Quarantined Rows",
        "",
        "| row_number | artist | row_type | previous_url | reasons |",
        "|---|---|---|---|---|",
    ]
    for item in payload.get("quarantined", [])[:300]:
        reasons = ", ".join(item.get("reason_codes", []))
        lines.append(
            f"| {item['row_number']} | {item['artist']} | {item['row_type']} | "
            f"{item['previous_url']} | {reasons} |"
        )
    if not payload.get("quarantined"):
        lines.append("| - | - | - | - | - |")
    lines.append("")
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    audit_payload = json.loads(args.audit_json.read_text(encoding="utf-8"))
    suspicious = [
        item
        for item in audit_payload.get("findings", [])
        if bool(item.get("quarantine_recommended"))
    ]
    suspicious_by_row = {int(item["row_number"]): item for item in suspicious}

    rows, fieldnames = load_csv()
    tracking = load_tracking()
    entries = tracking.get("entries", [])
    entries_by_row = {int(entry.get("row_number", 0)): entry for entry in entries if entry.get("row_number")}

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    quarantined: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=2):
        finding = suspicious_by_row.get(idx)
        if not finding:
            continue
        previous_url = (row.get("spotify_url") or "").strip()
        if not previous_url:
            continue
        quarantined.append(
            {
                "row_number": idx,
                "artist": row.get("artist", ""),
                "row_type": row.get("row_type", ""),
                "song_name": row.get("song_name", ""),
                "album_name": row.get("album_name", ""),
                "previous_url": previous_url,
                "reason_codes": finding.get("reason_codes", []),
                "severity": finding.get("severity", "medium"),
            }
        )
        row["spotify_url"] = ""

        entry = entries_by_row.get(idx)
        if entry:
            entry["spotify_status"] = "needs_research"
            entry["confidence_level"] = "low"
            entry["last_search_date"] = today
            history = entry.get("search_history", [])
            if not isinstance(history, list):
                history = []
            history.append(
                {
                    "date": today,
                    "round": "Spotify Quarantine",
                    "method": "mismatch_audit",
                    "result": "quarantined",
                    "previous_url": previous_url,
                    "reason_codes": finding.get("reason_codes", []),
                }
            )
            entry["search_history"] = history

    if args.write:
        safe_write_csv(CSV_PATH, rows, fieldnames)

        if tracking:
            total = len(entries)
            has_spotify_url = sum(1 for row in rows if (row.get("spotify_url") or "").strip())
            tracking["last_updated"] = datetime.now(timezone.utc).isoformat()
            if "statistics" not in tracking:
                tracking["statistics"] = {}
            tracking["statistics"]["total"] = total
            tracking["statistics"]["has_spotify_url"] = has_spotify_url
            tracking["statistics"]["needs_research"] = sum(
                1 for entry in entries if entry.get("spotify_status") == "needs_research"
            )
            tracking["statistics"]["not_searched"] = sum(
                1 for entry in entries if entry.get("spotify_status") == "not_searched"
            )
            TRACKING_PATH.write_text(json.dumps(tracking, indent=2), encoding="utf-8")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "write_mode": args.write,
        "rows_quarantined": len(quarantined),
        "quarantined": quarantined,
    }
    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    build_markdown(args.report_md, payload)

    print(f"rows_quarantined={len(quarantined)}")
    print(f"write_mode={str(args.write).lower()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
