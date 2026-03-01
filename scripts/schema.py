"""Canonical schema definitions for the P-Funk Archive.

Single source of truth for column lists, enum values, and shared
normalization functions used across all pipeline stages.
"""

from __future__ import annotations

import csv
import hashlib
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def load_env() -> None:
    """Load .env and .env.local from the project root (if present).

    Uses python-dotenv; existing env vars are NOT overridden.
    Safe to call multiple times.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for name in (".env", ".env.local"):
        env_path = _PROJECT_ROOT / name
        if env_path.exists():
            load_dotenv(env_path, override=False)


def require_env(name: str) -> str:
    """Return an environment variable or exit with a clear error."""
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


DISCOGRAPHY_COLUMNS = [
    "artist",
    "song_name",
    "album_name",
    "track_position",
    "row_type",
    "release_category",
    "edition_type",
    "version_type",
    "release_date",
    "label",
    "era",
    "genre",
    "chart_position",
    "awards",
    "spotify_url",
    "youtube_url",
    "duration_seconds",
    "alternative_names",
    "source_release_id",
    "work_id",
    "version_id",
    "notes",
]

VALID_ROW_TYPES = {"album", "track", "single"}

VALID_RELEASE_CATEGORIES = {
    "original",
    "reissue",
    "compilation",
    "live",
    "remix_album",
    "soundtrack",
}

VALID_EDITION_TYPES = {
    "standard",
    "expanded",
    "deluxe",
    "remaster",
    "remix",
    "demo",
    "bonus_track",
    "alternate_mix",
}

VALID_VERSION_TYPES = {
    "same_master",
    "remix_or_edit",
    "live_recording",
    "re_recording",
    "unknown",
}

VALID_ERAS = [
    "Pre-P-Funk (1955\u20131969)",
    "Classic P-Funk (1970\u20131981)",
    "Transition Era (1982\u20131992)",
    "Comeback Era (1993\u20132004)",
    "Late Career (2005\u20132015)",
    "Legacy Era (2016\u2013present)",
]

VALID_GENRES = {
    "Funk",
    "Soul",
    "R&B",
    "Psychedelic Rock",
    "Hip-Hop",
    "Electronic/Dance",
    "Jazz-Funk",
    "P-Funk",
    "Proto-Funk",
    "Neo-Funk",
    "Space Funk",
    "Boogie",
    "G-Funk",
    "Experimental",
}

LINEAGE_COLUMNS = [
    "work_id",
    "version_id",
    "version_type",
    "source_release_id",
    "duration_seconds",
]

DISCOVERY_SOURCE_COLUMNS = [
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

CANDIDATE_COLUMNS = [
    "artist",
    "album_name",
    "song_name",
    "release_date",
    "label",
    "row_type",
    "overall_confidence",
    "sources",
    "source_count",
    "status",
    "raw_extra",
]

# Version-type keywords used for inference from titles and notes.
_VERSION_KEYWORDS_RE = re.compile(
    r"\b(remix|mix|edit|extended|instrumental|dub|version)\b", re.IGNORECASE
)
_LIVE_KEYWORD_RE = re.compile(r"\blive\b", re.IGNORECASE)
_RE_RECORDING_RE = re.compile(
    r"\b(re-record|rerecord|new recording|alternate take)\b", re.IGNORECASE
)

# Regex for stripping version suffixes to find base work title.
_VERSION_SUFFIX_DASH_RE = re.compile(
    r"\b(remix|mix|edit|extended|instrumental|dub|version|live)\b", re.IGNORECASE
)
_VERSION_SUFFIX_PAREN_RE = re.compile(
    r"\(([^)]*(remix|mix|edit|extended|instrumental|dub|version|live)[^)]*)\)$",
    re.IGNORECASE,
)


def slug_hash(prefix: str, *parts: str) -> str:
    """Deterministic short hash used for stable IDs (art_, rel_, wrk_, etc.)."""
    key = "|".join(part.strip().lower() for part in parts)
    return f"{prefix}_{hashlib.sha1(key.encode('utf-8')).hexdigest()[:12]}"


def clean_title(value: str) -> str:
    return (value or "").strip()


def base_work_title(value: str) -> str:
    """Strip version/remix/live suffixes to get the canonical work title."""
    title = clean_title(value)
    parts = title.split(" - ", 1)
    if len(parts) == 2 and _VERSION_SUFFIX_DASH_RE.search(parts[1]):
        title = parts[0]
    title = _VERSION_SUFFIX_PAREN_RE.sub("", title).strip()
    return title or clean_title(value)


def infer_version_type(song_name: str, notes: str, current: str) -> str:
    """Infer version_type from title/notes if not already set."""
    if current in VALID_VERSION_TYPES:
        return current
    text = f"{song_name} {notes}".lower()
    if _VERSION_KEYWORDS_RE.search(text):
        return "remix_or_edit"
    if _LIVE_KEYWORD_RE.search(text):
        return "live_recording"
    if _RE_RECORDING_RE.search(text):
        return "re_recording"
    return "same_master"


def normalize_for_matching(text: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace for fuzzy matching."""
    s = (text or "").lower().strip()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def dedupe_key(row: dict[str, str]) -> tuple[str, ...]:
    """Canonical dedup key for discography rows."""
    return tuple(
        (row.get(col) or "").strip().lower()
        for col in [
            "artist",
            "song_name",
            "album_name",
            "track_position",
            "release_date",
            "row_type",
            "release_category",
            "edition_type",
        ]
    )


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------


class ProgressTracker:
    """Lightweight progress reporter for long-running loops.

    Usage:
        progress = ProgressTracker(total=len(items), noun="artists")
        for item in items:
            do_work(item)
            progress.update()
        progress.finish()

    Or as a context manager:
        with ProgressTracker(total=len(items), noun="rows") as p:
            for item in items:
                do_work(item)
                p.update()
    """

    def __init__(
        self,
        total: int,
        noun: str = "items",
        *,
        every: int = 0,
        every_seconds: float = 5.0,
    ) -> None:
        self.total = total
        self.noun = noun
        self.every = every or max(1, total // 20)
        self.every_seconds = every_seconds
        self.done = 0
        self._hits = 0
        self._start = time.monotonic()
        self._last_report = self._start

    def update(self, increment: int = 1, extra: str = "") -> None:
        self.done += increment
        now = time.monotonic()
        should_report = (
            self.done % self.every == 0
            or self.done == self.total
            or (now - self._last_report) >= self.every_seconds
        )
        if should_report:
            self._report(extra)
            self._last_report = now

    def _report(self, extra: str = "") -> None:
        elapsed = time.monotonic() - self._start
        pct = (self.done / self.total * 100) if self.total else 0
        rate = self.done / elapsed if elapsed > 0 else 0
        parts = [f"  [{pct:5.1f}%] {self.done}/{self.total} {self.noun}"]
        if elapsed >= 1:
            parts.append(f" ({rate:.1f}/s")
            if rate > 0 and self.done < self.total:
                remaining = (self.total - self.done) / rate
                if remaining >= 60:
                    parts.append(f", ~{remaining / 60:.1f}m left)")
                else:
                    parts.append(f", ~{remaining:.0f}s left)")
            else:
                parts.append(")")
        if extra:
            parts.append(f" {extra}")
        line = "".join(parts)
        sys.stderr.write(f"\r{line:<80}")
        sys.stderr.flush()
        self._hits += 1

    def finish(self, extra: str = "") -> None:
        elapsed = time.monotonic() - self._start
        if elapsed >= 60:
            elapsed_str = f"{elapsed / 60:.1f}m"
        else:
            elapsed_str = f"{elapsed:.1f}s"
        msg = f"  Done: {self.done} {self.noun} in {elapsed_str}"
        if extra:
            msg += f" {extra}"
        sys.stderr.write(f"\r{msg:<80}\n")
        sys.stderr.flush()

    def __enter__(self) -> "ProgressTracker":
        return self

    def __exit__(self, *args: object) -> None:
        if self.done > 0:
            self.finish()


# ---------------------------------------------------------------------------
# Input / output guardrails
# ---------------------------------------------------------------------------

def require_file(path: Path, label: str | None = None) -> None:
    """Verify a file exists and is non-empty, or exit with a clear message."""
    desc = label or str(path)
    if not path.exists():
        raise SystemExit(f"Required file missing: {desc}")
    if path.is_file() and path.stat().st_size == 0:
        raise SystemExit(f"Required file is empty: {desc}")


def validate_csv_input(
    path: Path,
    required_columns: list[str] | None = None,
    *,
    min_rows: int = 0,
) -> tuple[list[dict[str, str]], list[str]]:
    """Read a CSV, validate schema and row count, return (rows, fieldnames).

    Raises SystemExit with diagnostics on failure.
    """
    require_file(path)
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if required_columns and fieldnames != required_columns:
        missing = set(required_columns) - set(fieldnames)
        extra = set(fieldnames) - set(required_columns)
        parts = [f"Column mismatch in {path.name}."]
        if missing:
            parts.append(f"  missing: {sorted(missing)}")
        if extra:
            parts.append(f"  unexpected: {sorted(extra)}")
        raise SystemExit("\n".join(parts))

    if len(rows) < min_rows:
        raise SystemExit(
            f"{path.name} has {len(rows)} rows, expected at least {min_rows}"
        )
    return rows, fieldnames


def validate_csv_output(
    rows: list[dict[str, str]],
    fieldnames: list[str],
    *,
    target_path: Path | None = None,
    expected_columns: list[str] | None = None,
    min_rows: int = 1,
    max_shrink_pct: int = 10,
) -> None:
    """Pre-write check.  Raises SystemExit if output looks wrong."""
    if len(rows) < min_rows:
        raise SystemExit(
            f"Refusing to write: only {len(rows)} rows (min {min_rows})"
        )

    if expected_columns and fieldnames != expected_columns:
        missing = set(expected_columns) - set(fieldnames)
        raise SystemExit(
            f"Output column mismatch.  Missing: {sorted(missing)}"
        )

    if target_path and target_path.exists() and max_shrink_pct > 0:
        with target_path.open("r", encoding="utf-8") as f:
            existing_count = sum(1 for _ in f) - 1
        if existing_count > 0:
            shrink = (existing_count - len(rows)) / existing_count * 100
            if shrink > max_shrink_pct:
                raise SystemExit(
                    f"Refusing to write: row count would shrink by {shrink:.0f}% "
                    f"({existing_count} -> {len(rows)}, threshold {max_shrink_pct}%)"
                )


# ---------------------------------------------------------------------------
# Safe file I/O helpers
# ---------------------------------------------------------------------------

_BACKUP_SUFFIX = ".pre_pipeline_backup"
_INTEGRITY_PATH = Path(__file__).resolve().parent.parent / "data" / ".integrity.json"


def safe_write_csv(
    path: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str],
    *,
    backup: bool = True,
    expected_columns: list[str] | None = None,
    min_rows: int = 0,
) -> None:
    """Write CSV atomically with optional pre-write validation.

    Prevents partial writes from corrupting the target file.
    """
    if expected_columns or min_rows > 0:
        validate_csv_output(
            rows,
            fieldnames,
            target_path=path,
            expected_columns=expected_columns,
            min_rows=min_rows,
        )

    path.parent.mkdir(parents=True, exist_ok=True)

    if backup and path.exists():
        backup_path = path.with_suffix(path.suffix + _BACKUP_SUFFIX)
        shutil.copy2(path, backup_path)

    fd, tmp_path = tempfile.mkstemp(
        dir=path.parent, suffix=".tmp", prefix=path.stem + "_"
    )
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)
    except BaseException:
        os.unlink(tmp_path)
        raise

    if path.name == "discography.csv":
        save_integrity(path)


# ---------------------------------------------------------------------------
# Integrity checksums
# ---------------------------------------------------------------------------

def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def save_integrity(discography_path: Path) -> None:
    """Save SHA-256 checksum of discography.csv to .integrity.json."""
    import json
    from datetime import datetime, timezone

    row_count = 0
    with open(discography_path, "r", encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1  # subtract header

    payload = {
        "file": str(discography_path.name),
        "sha256": _file_sha256(discography_path),
        "row_count": row_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _INTEGRITY_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def verify_integrity(discography_path: Path) -> tuple[bool, str]:
    """Verify discography.csv matches saved checksum.

    Returns (ok, message).  If no integrity file exists, returns (True, "no baseline").
    """
    import json

    if not _INTEGRITY_PATH.exists():
        return True, "no integrity baseline (first run)"

    try:
        saved = json.loads(_INTEGRITY_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return True, "integrity file unreadable (treating as first run)"

    expected = saved.get("sha256", "")
    actual = _file_sha256(discography_path)

    if actual != expected:
        return False, (
            f"discography.csv was modified outside the pipeline "
            f"(expected {expected[:12]}..., got {actual[:12]}...)"
        )
    return True, "integrity check passed"
