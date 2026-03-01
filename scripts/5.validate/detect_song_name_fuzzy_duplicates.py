#!/usr/bin/env python3
"""Detect probable duplicate songs using fuzzy song_name matching."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schema import ProgressTracker  # noqa: E402


ROOT = Path(__file__).resolve().parent.parent.parent
CSV_PATH = ROOT / "data" / "discography.csv"
VERSION_HINTS = (
    "remix",
    "mix",
    "edit",
    "extended",
    "instrumental",
    "dub",
    "version",
    "live",
    "remaster",
    "radio",
    "mono",
    "stereo",
    "session",
    "recording",
    "rehearsal",
    "bonus",
)
TOKEN_STOPWORDS = {
    "a",
    "an",
    "and",
    "feat",
    "featuring",
    "ft",
    "for",
    "in",
    "of",
    "on",
    "part",
    "pt",
    "the",
    "to",
    "version",
}
LOW_SIGNAL_TOKENS = {"fire", "funk", "jam", "music", "song", "time"}
SAME_WORK_CLASSES = {"same_work_variant", "compilation_copy"}


@dataclass(frozen=True)
class Entry:
    idx: int
    row_number: int
    artist: str
    song_name: str
    album_name: str
    row_type: str
    release_date: str
    track_position: str
    release_category: str
    edition_type: str
    version_type: str
    is_compilation_track: bool
    source_release_id: str
    work_id: str
    spotify_url: str
    release_id: str
    normalized_full: str
    normalized_base: str
    token_set: frozenset[str]
    component_norms: tuple[str, ...]
    has_multi_title: bool


def clean_title(value: str) -> str:
    text = (value or "").strip()
    if text.startswith("[") and "] " in text:
        text = text.split("] ", 1)[1].strip()
    text = re.sub(r"\((feat\.|ft\.|featuring)\s+[^)]*\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def base_title(value: str) -> str:
    title = clean_title(value)
    parts = title.split(" - ", 1)
    if len(parts) == 2 and any(hint in parts[1].lower() for hint in VERSION_HINTS):
        title = parts[0].strip()
    title = re.sub(
        r"\(([^)]*(remix|mix|edit|extended|instrumental|dub|version|live|remaster|radio|mono|stereo)[^)]*)\)$",
        "",
        title,
        flags=re.IGNORECASE,
    ).strip()
    return title or clean_title(value)


def slug_hash(prefix: str, *parts: str) -> str:
    key = "|".join(part.strip().lower() for part in parts)
    return f"{prefix}_{hashlib.sha1(key.encode('utf-8')).hexdigest()[:12]}"


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


def normalize_for_compare(value: str) -> str:
    text = base_title(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\b(pt|part)\s*\.?\s*\d+\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(normalized: str) -> frozenset[str]:
    tokens = []
    for token in normalized.split():
        if token in TOKEN_STOPWORDS:
            continue
        if len(token) <= 1:
            continue
        tokens.append(token)
    return frozenset(tokens)


def split_components(song_name: str) -> tuple[str, ...]:
    title = clean_title(song_name)
    candidates = [title]

    if "/" in title:
        candidates.extend(part.strip() for part in re.split(r"\s*/\s*", title) if part.strip())

    normalized_parts = []
    seen = set()
    for candidate in candidates:
        norm = normalize_for_compare(candidate)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        normalized_parts.append(norm)
    return tuple(normalized_parts)


def parse_release_date(value: str) -> tuple[int, int, int]:
    value = (value or "").strip()
    if not value:
        return (9999, 99, 99)
    if re.fullmatch(r"\d{4}$", value):
        return (int(value), 99, 99)
    if re.fullmatch(r"\d{4}-\d{2}$", value):
        year, month = value.split("-")
        return (int(year), int(month), 99)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}$", value):
        year, month, day = value.split("-")
        return (int(year), int(month), int(day))
    return (9999, 99, 99)


def infer_subtype_tags(entry: Entry) -> list[str]:
    tags: list[str] = []
    title_text = clean_title(entry.song_name).lower()
    edition = (entry.edition_type or "").lower()
    version = (entry.version_type or "").lower()
    context = "live" if "live" in title_text else "studio"

    if "radio" in title_text or "radio" in edition:
        tags.append("radio_edit")
    if version == "remix_or_edit" or re.search(r"\b(remix|mix|edit|dub|extended|instrumental)\b", title_text):
        tags.append("remix")
    if edition == "remaster" or "remaster" in title_text:
        tags.append("remastered")
    if context == "live" or "live" in title_text:
        tags.append("live")
    if version == "re_recording":
        tags.append("re_recording")
    if entry.is_compilation_track and entry.source_release_id:
        tags.append("compilation_copy")

    deduped: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            deduped.append(tag)
    return deduped


def ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def token_sort_ratio(a_tokens: frozenset[str], b_tokens: frozenset[str]) -> float:
    if not a_tokens or not b_tokens:
        return 0.0
    a = " ".join(sorted(a_tokens))
    b = " ".join(sorted(b_tokens))
    return ratio(a, b)


def jaccard(a_tokens: frozenset[str], b_tokens: frozenset[str]) -> float:
    if not a_tokens or not b_tokens:
        return 0.0
    intersection = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    if union == 0:
        return 0.0
    return intersection / union


def best_component_ratio(a: Entry, b: Entry) -> float:
    best = 0.0
    for left in a.component_norms:
        for right in b.component_norms:
            best = max(best, ratio(left, right))
    return best


def has_meaningful_component_overlap(a: Entry, b: Entry, threshold: float) -> bool:
    if not (a.has_multi_title or b.has_multi_title):
        return False
    for left in a.component_norms:
        for right in b.component_norms:
            if ratio(left, right) < threshold:
                continue
            left_tokens = tokenize(left)
            right_tokens = tokenize(right)
            overlap = left_tokens & right_tokens
            # Avoid trivial one-token collisions like "jam".
            if len(overlap) >= 2:
                return True
            if len(overlap) == 1 and min(len(left_tokens), len(right_tokens)) >= 3:
                return True
    return False


def should_skip_pair(a: Entry, b: Entry) -> bool:
    if not a.normalized_base or not b.normalized_base:
        return True
    shorter = min(len(a.normalized_base), len(b.normalized_base))
    longer = max(len(a.normalized_base), len(b.normalized_base))
    if longer == 0:
        return True
    length_ratio = shorter / longer
    if length_ratio < 0.45 and "/" not in a.song_name and "/" not in b.song_name:
        return True
    if not (a.token_set & b.token_set):
        if a.normalized_base not in b.normalized_base and b.normalized_base not in a.normalized_base:
            return True
    return False


def classify_pair(
    a: Entry,
    b: Entry,
    *,
    min_score: float,
    component_score: float,
    include_exact: bool,
) -> dict[str, Any] | None:
    if should_skip_pair(a, b):
        return None

    same_artist = a.artist.strip().lower() == b.artist.strip().lower()
    reason_codes: list[str] = []

    if same_artist:
        reason_codes.append("same_artist")
    else:
        reason_codes.append("cross_artist")

    if a.normalized_base == b.normalized_base:
        if include_exact:
            reason_codes.append("exact_normalized_base")
            return {
                "match_type": "exact_normalized",
                "score": 1.0,
                "char_score": 1.0,
                "token_score": 1.0,
                "component_score": 1.0,
                "overlap_score": 1.0,
                "relation_class": "same_work_variant",
                "confidence_band": "high",
                "reason_codes": reason_codes + ["high_signal_match"],
            }
        return None

    char_score = ratio(a.normalized_base, b.normalized_base)
    token_score = token_sort_ratio(a.token_set, b.token_set)
    overlap_score = jaccard(a.token_set, b.token_set)
    shared_tokens = a.token_set & b.token_set
    comp_score = best_component_ratio(a, b)
    has_component_overlap = comp_score >= component_score and has_meaningful_component_overlap(
        a,
        b,
        component_score,
    )
    fuzzy_score = max(char_score, token_score)
    low_signal_collision = len(shared_tokens) == 1 and next(iter(shared_tokens)) in LOW_SIGNAL_TOKENS
    source_release_match = bool(a.source_release_id and a.source_release_id == b.source_release_id)
    release_to_source_match = bool(
        a.is_compilation_track and a.source_release_id and a.source_release_id == b.release_id
    ) or bool(
        b.is_compilation_track and b.source_release_id and b.source_release_id == a.release_id
    )
    source_equivalent = source_release_match or release_to_source_match
    subtype_overlap = set(infer_subtype_tags(a)) & set(infer_subtype_tags(b))

    if source_release_match:
        reason_codes.append("shared_source_release_id")
    if release_to_source_match:
        reason_codes.append("compilation_to_source_release_match")
    if same_artist and a.version_type and a.version_type == b.version_type:
        reason_codes.append("matching_version_type")
    if overlap_score >= 0.5:
        reason_codes.append("strong_token_overlap")
    if has_component_overlap:
        reason_codes.append("component_overlap")
    if subtype_overlap:
        reason_codes.append("shared_subtype_tags")

    base_candidate = False
    match_type = "fuzzy_title"
    score = fuzzy_score

    if has_component_overlap:
        base_candidate = True
        match_type = "component_overlap"
        score = comp_score
    if fuzzy_score >= min_score:
        if len(shared_tokens) >= 2:
            base_candidate = True
        if len(shared_tokens) == 1:
            token = next(iter(shared_tokens))
            if token in LOW_SIGNAL_TOKENS:
                base_candidate = False
            if (char_score >= 0.97 or token_score >= 0.97) and max(len(a.token_set), len(b.token_set)) <= 4:
                base_candidate = True
        if overlap_score >= 0.5 and (
            a.normalized_base in b.normalized_base or b.normalized_base in a.normalized_base
        ):
            base_candidate = True
            reason_codes.append("title_containment")

    if not base_candidate:
        return None

    if low_signal_collision:
        reason_codes.append("low_signal_overlap")

    relation_class = "uncertain"
    if source_equivalent and (a.is_compilation_track or b.is_compilation_track):
        relation_class = "compilation_copy"
    elif same_artist:
        relation_class = "same_work_variant"
    else:
        strong_same_work = 0
        if fuzzy_score >= 0.96:
            strong_same_work += 1
            reason_codes.append("high_title_similarity")
        if overlap_score >= 0.6:
            strong_same_work += 1
        if has_component_overlap:
            strong_same_work += 1
        if source_equivalent:
            strong_same_work += 2
        if a.spotify_url and b.spotify_url and a.spotify_url == b.spotify_url:
            strong_same_work += 2
            reason_codes.append("same_spotify_url")
        if strong_same_work >= 3:
            relation_class = "same_work_variant"
        elif fuzzy_score >= min_score:
            relation_class = "probable_cover_or_interpolation"
        else:
            relation_class = "uncertain"

    signal_count = 0
    if fuzzy_score >= 0.95:
        signal_count += 1
    if overlap_score >= 0.5:
        signal_count += 1
    if has_component_overlap:
        signal_count += 1
    if source_equivalent:
        signal_count += 1
    if a.version_type and b.version_type and a.version_type == b.version_type:
        signal_count += 1

    confidence_band = "low"
    if relation_class in SAME_WORK_CLASSES:
        if score >= 0.95 and signal_count >= 3 and not low_signal_collision:
            confidence_band = "high"
            reason_codes.append("high_signal_match")
        elif score >= 0.9 and signal_count >= 2:
            confidence_band = "medium"
    elif relation_class == "probable_cover_or_interpolation":
        confidence_band = "high" if score >= 0.95 else "medium"
    else:
        if score >= 0.92:
            confidence_band = "medium"

    return {
        "match_type": match_type,
        "score": score,
        "char_score": char_score,
        "token_score": token_score,
        "component_score": comp_score,
        "overlap_score": overlap_score,
        "relation_class": relation_class,
        "confidence_band": confidence_band,
        "reason_codes": sorted(set(reason_codes)),
    }


def load_entries(row_types: set[str]) -> list[Entry]:
    entries: list[Entry] = []
    with CSV_PATH.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader):
            row_type = (row.get("row_type", "") or "").strip().lower()
            if row_type not in row_types:
                continue
            song_name = row.get("song_name", "") or ""
            normalized_full = normalize_for_compare(song_name)
            normalized_base = normalized_full
            entries.append(
                Entry(
                    idx=len(entries),
                    row_number=idx + 2,
                    artist=(row.get("artist", "") or "").strip(),
                    song_name=song_name.strip(),
                    album_name=(row.get("album_name", "") or "").strip(),
                    row_type=row_type,
                    release_date=(row.get("release_date", "") or "").strip(),
                    track_position=(row.get("track_position", "") or "").strip(),
                    release_category=(row.get("release_category", "") or "").strip(),
                    edition_type=(row.get("edition_type", "") or "").strip(),
                    version_type=(row.get("version_type", "") or "").strip(),
                    is_compilation_track=row_type == "track"
                    and (row.get("release_category", "") or "").strip() == "compilation",
                    source_release_id=(row.get("source_release_id", "") or "").strip(),
                    work_id=(row.get("work_id", "") or "").strip(),
                    spotify_url=(row.get("spotify_url", "") or "").strip(),
                    release_id=release_id_for_row(row),
                    normalized_full=normalized_full,
                    normalized_base=normalized_base,
                    token_set=tokenize(normalized_base),
                    component_norms=split_components(song_name),
                    has_multi_title="/" in clean_title(song_name),
                )
            )
    return entries


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, x: int) -> int:
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: int, y: int) -> None:
        root_x = self.find(x)
        root_y = self.find(y)
        if root_x == root_y:
            return
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1


def original_rank_key(entry: Entry) -> tuple[Any, ...]:
    context = "live" if "live" in (entry.song_name or "").lower() else "studio"
    release_category = (entry.release_category or "").strip().lower()
    version_type = (entry.version_type or "").strip().lower()
    lexical_key = "|".join(
        [
            entry.artist.lower(),
            entry.song_name.lower(),
            entry.album_name.lower(),
            entry.release_date.lower(),
            entry.track_position.lower(),
            str(entry.row_number),
        ]
    )
    return (
        0 if context == "studio" else 1,
        0 if release_category != "compilation" else 1,
        0 if version_type == "same_master" else 1,
        parse_release_date(entry.release_date),
        0 if bool(entry.track_position) else 1,
        lexical_key,
    )


def build_cluster_id(members: list[Entry]) -> str:
    key = "|".join(str(item.row_number) for item in sorted(members, key=lambda item: item.row_number))
    return f"dup_{hashlib.sha1(key.encode('utf-8')).hexdigest()[:12]}"


def summarize_cluster(
    members: list[Entry],
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    ranked = sorted(members, key=original_rank_key)
    original = ranked[0]

    ranking_trace = []
    for entry in ranked:
        key = original_rank_key(entry)
        ranking_trace.append(
            {
                "row_number": entry.row_number,
                "artist": entry.artist,
                "song_name": entry.song_name,
                "release_date": entry.release_date,
                "release_category": entry.release_category,
                "version_type": entry.version_type,
                "rank_key": [
                    key[0],
                    key[1],
                    key[2],
                    list(key[3]),
                    key[4],
                    key[5],
                ],
            }
        )

    confidence_values = {pair.get("confidence_band", "low") for pair in pairs}
    cluster_confidence = "low"
    if "high" in confidence_values:
        cluster_confidence = "high"
    elif "medium" in confidence_values:
        cluster_confidence = "medium"

    relation_counts: dict[str, int] = {}
    for pair in pairs:
        relation = str(pair.get("relation_class", "uncertain"))
        relation_counts[relation] = relation_counts.get(relation, 0) + 1

    cluster_subtypes: set[str] = set()
    for member in members:
        cluster_subtypes.update(infer_subtype_tags(member))

    return {
        "cluster_id": build_cluster_id(members),
        "cluster_confidence": cluster_confidence,
        "pair_count": len(pairs),
        "relation_counts": relation_counts,
        "subtype_tags": sorted(cluster_subtypes),
        "original_candidate": {
            "row_number": original.row_number,
            "artist": original.artist,
            "song_name": original.song_name,
            "album_name": original.album_name,
            "release_date": original.release_date,
            "release_category": original.release_category,
            "version_type": original.version_type,
            "track_position": original.track_position,
        },
        "members": [
            {
                "row_number": entry.row_number,
                "artist": entry.artist,
                "song_name": entry.song_name,
                "album_name": entry.album_name,
                "release_date": entry.release_date,
                "release_category": entry.release_category,
                "edition_type": entry.edition_type,
                "version_type": entry.version_type,
                "source_release_id": entry.source_release_id,
                "work_id": entry.work_id,
            }
            for entry in sorted(members, key=lambda item: item.row_number)
        ],
        "ranking_trace": ranking_trace,
    }


def detect_candidates(
    entries: list[Entry],
    *,
    min_score: float,
    component_score: float,
    include_exact: bool,
    max_results: int,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    match_counts = {
        "match_type": {"exact_normalized": 0, "fuzzy_title": 0, "component_overlap": 0},
        "relation_class": {
            "same_work_variant": 0,
            "compilation_copy": 0,
            "probable_cover_or_interpolation": 0,
            "uncertain": 0,
        },
        "confidence_band": {"high": 0, "medium": 0, "low": 0},
    }
    uf = UnionFind(len(entries))
    n = len(entries)
    total_pairs = n * (n - 1) // 2
    progress = ProgressTracker(total=n, noun="rows (O(n^2) scan)")

    for i in range(n):
        left = entries[i]
        for j in range(i + 1, n):
            right = entries[j]
            verdict = classify_pair(
                left,
                right,
                min_score=min_score,
                component_score=component_score,
                include_exact=include_exact,
            )
            if not verdict:
                continue
            match_type = str(verdict["match_type"])
            score = float(verdict["score"])
            relation_class = str(verdict["relation_class"])
            confidence_band = str(verdict["confidence_band"])
            match_counts["match_type"][match_type] = match_counts["match_type"].get(match_type, 0) + 1
            match_counts["relation_class"][relation_class] = (
                match_counts["relation_class"].get(relation_class, 0) + 1
            )
            match_counts["confidence_band"][confidence_band] = (
                match_counts["confidence_band"].get(confidence_band, 0) + 1
            )
            if relation_class in SAME_WORK_CLASSES and confidence_band == "high":
                uf.union(left.idx, right.idx)
            shared_tokens = sorted(left.token_set & right.token_set)
            shared_subtypes = sorted(set(infer_subtype_tags(left)) & set(infer_subtype_tags(right)))
            candidates.append(
                {
                    "score": round(score, 4),
                    "match_type": match_type,
                    "relation_class": relation_class,
                    "confidence_band": confidence_band,
                    "reason_codes": verdict["reason_codes"],
                    "char_score": round(float(verdict["char_score"]), 4),
                    "token_score": round(float(verdict["token_score"]), 4),
                    "component_score": round(float(verdict["component_score"]), 4),
                    "overlap_score": round(float(verdict["overlap_score"]), 4),
                    "shared_tokens": shared_tokens,
                    "shared_subtype_tags": shared_subtypes,
                    "left": {
                        "row_number": left.row_number,
                        "artist": left.artist,
                        "song_name": left.song_name,
                        "album_name": left.album_name,
                        "release_date": left.release_date,
                        "row_type": left.row_type,
                        "release_category": left.release_category,
                        "edition_type": left.edition_type,
                        "version_type": left.version_type,
                        "source_release_id": left.source_release_id,
                        "release_id": left.release_id,
                    },
                    "right": {
                        "row_number": right.row_number,
                        "artist": right.artist,
                        "song_name": right.song_name,
                        "album_name": right.album_name,
                        "release_date": right.release_date,
                        "row_type": right.row_type,
                        "release_category": right.release_category,
                        "edition_type": right.edition_type,
                        "version_type": right.version_type,
                        "source_release_id": right.source_release_id,
                        "release_id": right.release_id,
                    },
                }
            )
        progress.update(extra=f"| {len(candidates)} candidates so far")
    progress.finish(extra=f"| {len(candidates)} candidate pairs")

    candidates.sort(
        key=lambda item: (
            -float(item["score"]),
            str(item["match_type"]),
            str(item["left"]["song_name"]),
            str(item["right"]["song_name"]),
        )
    )
    if max_results > 0:
        candidates = candidates[:max_results]

    groups: dict[int, set[int]] = {}
    for entry in entries:
        root = uf.find(entry.idx)
        groups.setdefault(root, set()).add(entry.idx)
    row_to_entry = {entry.idx: entry for entry in entries}

    row_number_to_idx = {entry.row_number: entry.idx for entry in entries}
    pair_index: dict[tuple[int, int], dict[str, Any]] = {}
    for candidate in candidates:
        left_idx = row_number_to_idx[int(candidate["left"]["row_number"])]
        right_idx = row_number_to_idx[int(candidate["right"]["row_number"])]
        pair_index[(min(left_idx, right_idx), max(left_idx, right_idx))] = candidate

    clusters: list[dict[str, Any]] = []
    for indices in groups.values():
        if len(indices) <= 1:
            continue
        cluster_indices = sorted(indices)
        members = [row_to_entry[item] for item in cluster_indices]
        cluster_pairs = []
        for i in range(len(cluster_indices)):
            for j in range(i + 1, len(cluster_indices)):
                key = (cluster_indices[i], cluster_indices[j])
                candidate = pair_index.get(key)
                if not candidate:
                    continue
                if candidate["relation_class"] in SAME_WORK_CLASSES:
                    cluster_pairs.append(candidate)
        if not cluster_pairs:
            continue
        clusters.append(summarize_cluster(members, cluster_pairs))

    clusters.sort(key=lambda item: len(item["members"]), reverse=True)
    return candidates, match_counts, clusters


def write_markdown(
    path: Path,
    *,
    scanned_rows: int,
    row_types: set[str],
    min_score: float,
    component_score: float,
    include_exact: bool,
    candidates: list[dict[str, Any]],
    match_counts: dict[str, dict[str, int]],
    clusters: list[dict[str, Any]],
) -> None:
    high_confidence_pairs = [item for item in candidates if item.get("confidence_band") == "high"]
    high_confidence_clusters = [item for item in clusters if item.get("cluster_confidence") == "high"]

    lines = [
        "# Song Name Fuzzy Duplicate Report",
        "",
        f"- generated_at: {datetime.now(timezone.utc).isoformat()}",
        f"- scanned_rows: {scanned_rows}",
        f"- row_types: {', '.join(sorted(row_types))}",
        f"- min_score: {min_score}",
        f"- component_score: {component_score}",
        f"- include_exact: {str(include_exact).lower()}",
        f"- candidate_pairs: {len(candidates)}",
        f"- cluster_count: {len(clusters)}",
        f"- high_confidence_pairs: {len(high_confidence_pairs)}",
        f"- high_confidence_clusters: {len(high_confidence_clusters)}",
        "",
        "## Match Type Counts",
        "",
        f"- exact_normalized: {match_counts['match_type'].get('exact_normalized', 0)}",
        f"- fuzzy_title: {match_counts['match_type'].get('fuzzy_title', 0)}",
        f"- component_overlap: {match_counts['match_type'].get('component_overlap', 0)}",
        "",
        "## Relation Class Counts",
        "",
        f"- same_work_variant: {match_counts['relation_class'].get('same_work_variant', 0)}",
        f"- compilation_copy: {match_counts['relation_class'].get('compilation_copy', 0)}",
        f"- probable_cover_or_interpolation: "
        f"{match_counts['relation_class'].get('probable_cover_or_interpolation', 0)}",
        f"- uncertain: {match_counts['relation_class'].get('uncertain', 0)}",
        "",
        "## Confidence Band Counts",
        "",
        f"- high: {match_counts['confidence_band'].get('high', 0)}",
        f"- medium: {match_counts['confidence_band'].get('medium', 0)}",
        f"- low: {match_counts['confidence_band'].get('low', 0)}",
        "",
        "## Top Candidate Pairs",
        "",
        "| score | type | relation | confidence | left | right | reasons |",
        "|---|---|---|---|---|---|---|",
    ]

    for item in candidates[:200]:
        left = item["left"]
        right = item["right"]
        left_label = f"{left['artist']} - {left['song_name']}"
        right_label = f"{right['artist']} - {right['song_name']}"
        reasons = ", ".join(item["reason_codes"][:4]) if item["reason_codes"] else "-"
        lines.append(
            f"| {item['score']:.4f} | {item['match_type']} | {item['relation_class']} | "
            f"{item['confidence_band']} | {left_label} | {right_label} | {reasons} |"
        )

    lines.append("")
    lines.append("## High Confidence Cluster Originals")
    lines.append("")
    lines.append("| cluster_id | confidence | original_candidate | members |")
    lines.append("|---|---|---|---|")
    for cluster in high_confidence_clusters[:100]:
        original = cluster["original_candidate"]
        original_label = f"{original['artist']} - {original['song_name']} ({original['release_date']})"
        lines.append(
            f"| {cluster['cluster_id']} | {cluster['cluster_confidence']} | {original_label} | "
            f"{len(cluster['members'])} |"
        )
    if not high_confidence_clusters:
        lines.append("| - | - | - | - |")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `probable_cover_or_interpolation` pairs are cross-artist links and not merge candidates.")
    lines.append("- Original selection uses stable ranking across context/category/version/date signals.")
    lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def write_high_confidence_reports(
    report_json: Path,
    report_md: Path,
    *,
    clusters: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> None:
    high_clusters = [item for item in clusters if item.get("cluster_confidence") == "high"]
    high_pairs = [
        item
        for item in candidates
        if item.get("confidence_band") == "high" and item.get("relation_class") in SAME_WORK_CLASSES
    ]
    high_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "high_confidence_pair_count": len(high_pairs),
            "high_confidence_cluster_count": len(high_clusters),
            "clusters_missing_original": sum(1 for item in high_clusters if not item.get("original_candidate")),
        },
        "clusters": high_clusters,
        "pairs": high_pairs,
    }

    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(high_payload, indent=2), encoding="utf-8")

    lines = [
        "# High Confidence Duplicate Candidates",
        "",
        f"- generated_at: {high_payload['generated_at']}",
        f"- high_confidence_pair_count: {high_payload['metrics']['high_confidence_pair_count']}",
        f"- high_confidence_cluster_count: {high_payload['metrics']['high_confidence_cluster_count']}",
        "",
        "## Original Candidates",
        "",
        "| cluster_id | original | release_date | members |",
        "|---|---|---|---|",
    ]
    for cluster in high_clusters:
        original = cluster["original_candidate"]
        lines.append(
            f"| {cluster['cluster_id']} | {original['artist']} - {original['song_name']} | "
            f"{original['release_date']} | {len(cluster['members'])} |"
        )
    if not high_clusters:
        lines.append("| - | - | - | - |")

    lines.extend(["", "## Top High-Confidence Pairs", "", "| score | relation | left | right |", "|---|---|---|---|"])
    for pair in high_pairs[:200]:
        lines.append(
            f"| {pair['score']:.4f} | {pair['relation_class']} | "
            f"{pair['left']['artist']} - {pair['left']['song_name']} | "
            f"{pair['right']['artist']} - {pair['right']['song_name']} |"
        )
    if not high_pairs:
        lines.append("| - | - | - | - |")
    lines.append("")
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect fuzzy duplicate song_name pairs")
    parser.add_argument("--report-json", type=Path, required=True)
    parser.add_argument("--report-md", type=Path, required=True)
    parser.add_argument("--high-confidence-json", type=Path, default=None)
    parser.add_argument("--high-confidence-md", type=Path, default=None)
    parser.add_argument("--row-types", default="track,single", help="Comma-separated row types")
    parser.add_argument("--min-score", type=float, default=0.88)
    parser.add_argument("--component-score", type=float, default=0.92)
    parser.add_argument("--include-exact", action="store_true")
    parser.add_argument("--max-results", type=int, default=2000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    row_types = {part.strip().lower() for part in args.row_types.split(",") if part.strip()}
    entries = load_entries(row_types=row_types)
    candidates, match_counts, clusters = detect_candidates(
        entries,
        min_score=args.min_score,
        component_score=args.component_score,
        include_exact=args.include_exact,
        max_results=args.max_results,
    )
    if args.high_confidence_json is None:
        args.high_confidence_json = args.report_json.parent / "high_confidence_candidates.json"
    if args.high_confidence_md is None:
        args.high_confidence_md = args.report_md.parent / "high_confidence_candidates.md"

    high_confidence_pairs = [
        item
        for item in candidates
        if item.get("confidence_band") == "high" and item.get("relation_class") in SAME_WORK_CLASSES
    ]
    high_confidence_clusters = [item for item in clusters if item.get("cluster_confidence") == "high"]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "settings": {
            "row_types": sorted(row_types),
            "min_score": args.min_score,
            "component_score": args.component_score,
            "include_exact": args.include_exact,
            "max_results": args.max_results,
        },
        "metrics": {
            "scanned_rows": len(entries),
            "candidate_pairs": len(candidates),
            "cluster_count": len(clusters),
            "high_confidence_pair_count": len(high_confidence_pairs),
            "high_confidence_cluster_count": len(high_confidence_clusters),
            "match_type_counts": match_counts,
        },
        "clusters": clusters,
        "candidates": candidates,
    }

    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_markdown(
        args.report_md,
        scanned_rows=len(entries),
        row_types=row_types,
        min_score=args.min_score,
        component_score=args.component_score,
        include_exact=args.include_exact,
        candidates=candidates,
        match_counts=match_counts,
        clusters=clusters,
    )
    write_high_confidence_reports(
        args.high_confidence_json,
        args.high_confidence_md,
        clusters=clusters,
        candidates=candidates,
    )

    print(f"scanned_rows={len(entries)}")
    print(f"candidate_pairs={len(candidates)}")
    print(f"cluster_count={len(clusters)}")
    print(f"high_confidence_pair_count={len(high_confidence_pairs)}")
    print(f"high_confidence_cluster_count={len(high_confidence_clusters)}")
    print(f"match_type_counts={json.dumps(match_counts, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
