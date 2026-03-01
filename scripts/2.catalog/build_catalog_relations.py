#!/usr/bin/env python3
"""Build stable-ID relational catalog files from discography.csv."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import (  # noqa: E402
    DISCOGRAPHY_COLUMNS,
    slug_hash,
    clean_title,
    base_work_title,
    infer_version_type,
    validate_csv_input,
)


ROOT = Path(__file__).resolve().parent.parent.parent
DISCOGRAPHY = ROOT / "data" / "discography.csv"
ARTISTS_OUT = ROOT / "data" / "catalog_artists.csv"
RELEASES_OUT = ROOT / "data" / "catalog_releases.csv"
WORKS_OUT = ROOT / "data" / "catalog_works.csv"
TRACKS_OUT = ROOT / "data" / "catalog_tracks.csv"


def load_rows() -> list[dict[str, str]]:
    rows, _ = validate_csv_input(DISCOGRAPHY, DISCOGRAPHY_COLUMNS, min_rows=1)
    return rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rows = load_rows()

    artist_map: dict[str, dict[str, str]] = {}
    release_map: dict[tuple[str, ...], dict[str, str]] = {}
    work_map: dict[tuple[str, ...], dict[str, str]] = {}
    track_rows: list[dict[str, str]] = []
    source_candidates: dict[str, list[tuple[str, str]]] = {}
    source_candidates_by_artist: dict[str, list[tuple[str, str]]] = {}

    for row in rows:
        artist = row.get("artist", "").strip()
        artist_id = slug_hash("art", artist)
        if artist_id not in artist_map:
            artist_map[artist_id] = {
                "artist_id": artist_id,
                "artist_name": artist,
                "artist_name_normalized": artist.lower(),
            }

        album_name = row.get("album_name", "").strip()
        release_date = row.get("release_date", "").strip()
        release_category = row.get("release_category", "").strip()
        edition_type = row.get("edition_type", "").strip()
        release_key = (artist_id, album_name.lower(), release_date, release_category, edition_type)
        release_id = slug_hash("rel", *release_key)
        if release_key not in release_map:
            release_map[release_key] = {
                "release_id": release_id,
                "artist_id": artist_id,
                "artist_name": artist,
                "album_name": album_name,
                "release_date": release_date,
                "release_category": release_category,
                "edition_type": edition_type,
                "era": row.get("era", "").strip(),
                "genre": row.get("genre", "").strip(),
            }

        song_name = row.get("song_name", "").strip()
        clean_song = clean_title(song_name)
        base_title = base_work_title(song_name)
        work_key = (artist_id, base_title.lower())
        work_id = row.get("work_id", "").strip() or slug_hash("wrk", *work_key)
        if work_key not in work_map:
            work_map[work_key] = {
                "work_id": work_id,
                "artist_id": artist_id,
                "artist_name": artist,
                "work_name": base_title,
            }

        version_type = infer_version_type(
            song_name=row.get("song_name", ""),
            notes=row.get("notes", ""),
            current=row.get("version_type", "").strip(),
        )
        version_id = row.get("version_id", "").strip() or slug_hash(
            "ver",
            work_id,
            clean_song.lower(),
            row.get("duration_seconds", ""),
        )
        track_id = slug_hash(
            "trk",
            artist_id,
            clean_song.lower(),
            album_name.lower(),
            row.get("track_position", "").strip(),
            release_date,
            release_category,
            edition_type,
        )
        track_rows.append(
            {
                "track_id": track_id,
                "release_id": release_id,
                "work_id": work_id,
                "version_id": version_id,
                "artist_id": artist_id,
                "artist_name": artist,
                "song_name": clean_song,
                "album_name": album_name,
                "track_position": row.get("track_position", "").strip(),
                "row_type": row.get("row_type", "").strip(),
                "release_date": release_date,
                "release_category": release_category,
                "edition_type": edition_type,
                "version_type": version_type,
                "source_release_id": row.get("source_release_id", "").strip(),
                "duration_seconds": row.get("duration_seconds", "").strip(),
                "spotify_url": row.get("spotify_url", "").strip(),
                "youtube_url": row.get("youtube_url", "").strip(),
            }
        )

        if release_category != "compilation":
            source_candidates.setdefault(work_id, []).append((release_date, release_id))
            source_candidates_by_artist.setdefault(artist_id, []).append((release_date, release_id))

    # Fill missing source_release_id deterministically for compilation tracks.
    best_source: dict[str, str] = {}
    for work_id, candidates in source_candidates.items():
        ordered = sorted(candidates, key=lambda item: item[0] or "9999")
        best_source[work_id] = ordered[0][1]
    best_artist_source: dict[str, str] = {}
    for artist_id, candidates in source_candidates_by_artist.items():
        ordered = sorted(candidates, key=lambda item: item[0] or "9999")
        best_artist_source[artist_id] = ordered[0][1]
    for row in track_rows:
        is_compilation_track = (
            row["row_type"] == "track" and row["release_category"] == "compilation"
        )
        if is_compilation_track and not row["source_release_id"]:
            row["source_release_id"] = best_source.get(row["work_id"], "")
            if not row["source_release_id"]:
                row["source_release_id"] = best_artist_source.get(row["artist_id"], "")

    write_csv(
        ARTISTS_OUT,
        ["artist_id", "artist_name", "artist_name_normalized"],
        sorted(artist_map.values(), key=lambda item: item["artist_name_normalized"]),
    )
    write_csv(
        RELEASES_OUT,
        [
            "release_id",
            "artist_id",
            "artist_name",
            "album_name",
            "release_date",
            "release_category",
            "edition_type",
            "era",
            "genre",
        ],
        sorted(release_map.values(), key=lambda item: (item["artist_name"].lower(), item["album_name"].lower())),
    )
    write_csv(
        WORKS_OUT,
        ["work_id", "artist_id", "artist_name", "work_name"],
        sorted(work_map.values(), key=lambda item: (item["artist_name"].lower(), item["work_name"].lower())),
    )
    write_csv(
        TRACKS_OUT,
        [
            "track_id",
            "release_id",
            "work_id",
            "version_id",
            "artist_id",
            "artist_name",
            "song_name",
            "album_name",
            "track_position",
            "row_type",
            "release_date",
            "release_category",
            "edition_type",
            "version_type",
            "source_release_id",
            "duration_seconds",
            "spotify_url",
            "youtube_url",
        ],
        track_rows,
    )

    print(f"artists={len(artist_map)}")
    print(f"releases={len(release_map)}")
    print(f"works={len(work_map)}")
    print(f"tracks={len(track_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
