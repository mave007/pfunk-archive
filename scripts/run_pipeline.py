#!/usr/bin/env python3
"""Pipeline runner with dependency graph, parallel execution, and safety checks.

Usage:
    python3 scripts/run_pipeline.py                     # full pipeline
    python3 scripts/run_pipeline.py --stage 0            # only stage 0
    python3 scripts/run_pipeline.py --stage 4 --stage 5  # stages 4 and 5
    python3 scripts/run_pipeline.py --dry-run             # show plan without executing
    python3 scripts/run_pipeline.py --skip-api            # skip stages that need API keys
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schema import verify_integrity, save_integrity  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
REPORTS = ROOT / "reports"
DISCOGRAPHY = ROOT / "data" / "discography.csv"


@dataclass
class Step:
    name: str
    command: list[str]
    stage: int
    requires_api: bool = False
    required_files: list[str] = field(default_factory=list)


STAGE_0_SCRAPERS = [
    Step("scrape_wikipedia", ["python3", str(SCRIPTS / "0.discover/scrape_wikipedia_pfunk.py")], 0),
    Step("scrape_motherpage", ["python3", str(SCRIPTS / "0.discover/scrape_motherpage.py")], 0),
    Step("scrape_georgeclinton", ["python3", str(SCRIPTS / "0.discover/scrape_georgeclinton.py")], 0),
    Step("scrape_pfunk_forums", ["python3", str(SCRIPTS / "0.discover/scrape_pfunk_forums.py")], 0),
    Step("discover_from_discogs", ["python3", str(SCRIPTS / "0.discover/discover_from_discogs.py")], 0,
         requires_api=True, required_files=["data/discovery_seeds.csv"]),
    Step("discover_from_spotify", ["python3", str(SCRIPTS / "0.discover/discover_from_spotify.py")], 0,
         requires_api=True),
    Step("discover_from_musicbrainz", ["python3", str(SCRIPTS / "0.discover/discover_from_musicbrainz.py")], 0,
         required_files=["data/discovery_seeds.csv"]),
    Step("discover_from_wikidata", ["python3", str(SCRIPTS / "0.discover/discover_from_wikidata.py")], 0),
]

STAGE_0_POST = [
    Step("consolidate_candidates", ["python3", str(SCRIPTS / "0.discover/consolidate_candidates.py")], 0),
    Step("merge_candidates", ["python3", str(SCRIPTS / "0.discover/merge_candidates.py"), "--write"], 0,
         required_files=["data/discovery_candidates.csv"]),
]

STAGE_1 = [
    Step("canonicalize", [
        "python3", str(SCRIPTS / "1.normalize/canonicalize_discography.py"), "--write",
        "--report-json", str(REPORTS / "canonicalization/latest.json"),
        "--report-md", str(REPORTS / "canonicalization/latest.md"),
    ], 1, required_files=["data/discography.csv"]),
]

STAGE_2 = [
    Step("build_catalog", ["python3", str(SCRIPTS / "2.catalog/build_catalog_relations.py")], 2,
         required_files=["data/discography.csv"]),
]

STAGE_3 = [
    Step("reconcile_tracking", [
        "python3", str(SCRIPTS / "3.reconcile/reconcile_tracking.py"),
        "--report-json", str(REPORTS / "reconciliation/latest.json"), "--write",
    ], 3, required_files=["data/discography.csv"]),
]

STAGE_4_PARALLEL = [
    Step("enrich_spotify", ["python3", str(SCRIPTS / "4.enrich/enrich_spotify.py")], 4,
         requires_api=True, required_files=["data/discography.csv"]),
    Step("enrich_youtube", ["python3", str(SCRIPTS / "4.enrich/enrich_youtube.py"), "--limit", "95"], 4,
         requires_api=True, required_files=["data/discography.csv"]),
    Step("enrich_personnel", ["python3", str(SCRIPTS / "4.enrich/enrich_personnel_from_discogs.py")], 4,
         requires_api=True),
]

STAGE_4_BACKFILLS = [
    Step("backfill_duration", ["python3", str(SCRIPTS / "4.enrich/backfill_duration_from_cache.py")], 4,
         required_files=["data/discography.csv"]),
    Step("backfill_spotify", ["python3", str(SCRIPTS / "4.enrich/backfill_spotify_from_cache.py")], 4,
         required_files=["data/discography.csv"]),
]

STAGE_5_VALIDATORS = [
    Step("fuzzy_duplicates", [
        "python3", str(SCRIPTS / "5.validate/detect_song_name_fuzzy_duplicates.py"),
        "--report-json", str(REPORTS / "duplicates/song_name_fuzzy_latest.json"),
        "--report-md", str(REPORTS / "duplicates/song_name_fuzzy_latest.md"),
        "--high-confidence-json", str(REPORTS / "duplicates/high_confidence_candidates.json"),
        "--high-confidence-md", str(REPORTS / "duplicates/high_confidence_candidates.md"),
    ], 5, required_files=["data/discography.csv"]),
    Step("quality_gates", [
        "python3", str(SCRIPTS / "5.validate/quality_gates.py"),
        "--report-json", str(REPORTS / "quality/quality_latest.json"),
        "--report-md", str(REPORTS / "quality/quality_latest.md"),
    ], 5, required_files=["data/discography.csv"]),
    Step("validate_discography", ["python3", str(SCRIPTS / "5.validate/validate_discography.py")], 5,
         required_files=["data/discography.csv"]),
    Step("validate_schema", ["python3", str(SCRIPTS / "5.validate/validate_schema.py")], 5,
         required_files=["data/discography.csv"]),
]


def check_preconditions(step: Step) -> str | None:
    """Return an error message if preconditions fail, None if OK."""
    for rel_path in step.required_files:
        full_path = ROOT / rel_path
        if not full_path.exists():
            return f"required file missing: {rel_path}"
    return None


def run_step(step: Step) -> tuple[str, int, float, str]:
    """Run a step and return (name, exit_code, elapsed_seconds, output)."""
    start = time.time()
    try:
        result = subprocess.run(
            step.command,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=600,
        )
        elapsed = time.time() - start
        output = result.stdout + result.stderr
        return step.name, result.returncode, elapsed, output.strip()
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        return step.name, -1, elapsed, "TIMEOUT after 600s"
    except Exception as exc:
        elapsed = time.time() - start
        return step.name, -1, elapsed, str(exc)


def run_parallel(steps: list[Step], skip_api: bool) -> list[tuple[str, int, float, str]]:
    """Run steps in parallel, skipping API steps if requested."""
    filtered = [s for s in steps if not (skip_api and s.requires_api)]
    if not filtered:
        return []

    results = []
    with ProcessPoolExecutor(max_workers=min(len(filtered), 6)) as executor:
        futures = {executor.submit(run_step, s): s for s in filtered}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def run_sequential(steps: list[Step], skip_api: bool) -> list[tuple[str, int, float, str]]:
    """Run steps sequentially, skipping API steps if requested."""
    results = []
    for step in steps:
        if skip_api and step.requires_api:
            continue
        err = check_preconditions(step)
        if err:
            results.append((step.name, -1, 0.0, f"SKIPPED: {err}"))
            continue
        results.append(run_step(step))
    return results


def print_results(label: str, results: list[tuple[str, int, float, str]]) -> bool:
    """Print results and return True if all passed."""
    if not results:
        return True
    all_ok = True
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    for name, code, elapsed, output in results:
        status = "OK" if code == 0 else "FAIL" if code > 0 else "SKIP"
        if code != 0:
            all_ok = False
        print(f"  [{status}] {name} ({elapsed:.1f}s)")
        if code != 0 and output:
            for line in output.split("\n")[:5]:
                print(f"         {line}")
    return all_ok


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run P-Funk Archive pipeline")
    parser.add_argument("--stage", type=int, action="append", help="Run specific stage(s) only")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without executing")
    parser.add_argument("--skip-api", action="store_true", help="Skip steps requiring API keys")
    return parser.parse_args()


def should_run(stage: int, requested: list[int] | None) -> bool:
    return requested is None or stage in requested


def main() -> int:
    args = parse_args()
    requested = args.stage
    total_start = time.time()
    all_ok = True

    stages = [
        (0, "Stage 0: Discovery (scrapers)", STAGE_0_SCRAPERS, True),
        (0, "Stage 0: Discovery (consolidate + merge)", STAGE_0_POST, False),
        (1, "Stage 1: Normalize", STAGE_1, False),
        (2, "Stage 2: Build Catalog", STAGE_2, False),
        (3, "Stage 3: Reconcile", STAGE_3, False),
        (4, "Stage 4: Enrich (API)", STAGE_4_PARALLEL, False),
        (4, "Stage 4: Enrich (backfills)", STAGE_4_BACKFILLS, False),
        (5, "Stage 5: Validate", STAGE_5_VALIDATORS, True),
    ]

    if args.dry_run:
        print("Pipeline plan (dry-run):\n")
        for stage_num, label, steps, parallel in stages:
            if not should_run(stage_num, requested):
                continue
            mode = "parallel" if parallel else "sequential"
            print(f"  {label} ({mode}):")
            for s in steps:
                api_tag = " [API]" if s.requires_api else ""
                skip_tag = " [SKIP]" if args.skip_api and s.requires_api else ""
                print(f"    - {s.name}{api_tag}{skip_tag}")
        return 0

    if DISCOGRAPHY.exists():
        ok, msg = verify_integrity(DISCOGRAPHY)
        if not ok:
            print(f"\n  WARNING: {msg}")
            print("  The file may have been edited manually. Proceeding anyway.\n")

    for stage_num, label, steps, parallel in stages:
        if not should_run(stage_num, requested):
            continue
        if parallel:
            results = run_parallel(steps, args.skip_api)
        else:
            results = run_sequential(steps, args.skip_api)
        stage_ok = print_results(label, results)
        if not stage_ok:
            all_ok = False
            if stage_num < 5:
                print(f"\n  WARNING: {label} had failures; continuing to next stage")

    elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"  Pipeline {'PASSED' if all_ok else 'COMPLETED WITH ERRORS'} in {elapsed:.1f}s")
    print(f"{'='*60}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
