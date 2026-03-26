#!/usr/bin/env python3
"""Group the Bruker S3 bucket file listing into logical datasets.

Reads the TSV produced by fetch_bruker.py and writes:
  - registry/bruker_datasets.json  (grouped datasets with file listings)
  - registry/bruker_status.csv     (curation status; preserves existing rows)

Usage:
    python tools/group_bruker.py [files_tsv] [datasets_json] [status_csv]
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

FILES_DEFAULT = "registry/bruker_files.txt"
JSON_DEFAULT = "registry/bruker_datasets.json"
STATUS_DEFAULT = "registry/bruker_status.csv"

BUCKET_URL = "https://smi-public.objects.liquidweb.services/"
WTX_MANUSCRIPT = "wtx_manuscript"

# Regex rules for top-level files (no "/" in key). Order matters.
TOP_LEVEL_RULES: list[tuple[re.Pattern[str], str]] = [
    # Mouse brain must match before generic brain
    (re.compile(r"^mu[\s_-]*brain", re.IGNORECASE), "mouse_brain"),
    # Human brain datasets (including Half Brain, Quarter Brain)
    (re.compile(r"^(brain|halfbrain|half[\s_-]+brain|quarterbrain|quarter[\s_-]+brain|braindata|brainrelease)", re.IGNORECASE), "brain"),
    # Liver datasets
    (re.compile(r"^(liver|normalliver|normal[\s_-]+liver)", re.IGNORECASE), "liver"),
    # Logs
    (re.compile(r"^logs\.", re.IGNORECASE), "misc"),
]

STATUS_FIELDS = ["dataset_id", "display_name", "downloaded", "converted_to_spatialdata", "uploaded_to_lamin", "notes"]


def make_display_name(group_id: str) -> str:
    """Convert a group_id to a human-readable display name.

    'wtx_manuscript/37CPA_rep_1' -> '37CPA Rep 1'
    '6k_release' -> '6K Release'
    'cosmx-wtx' -> 'CosMx WTx'
    """
    name = group_id.split("/")[-1]
    name = name.replace("_", " ").replace("-", " ")
    # Title-case while preserving consecutive uppercase (e.g. "CPA", "WTx")
    parts = name.split()
    titled = []
    for p in parts:
        if p.upper() == p and len(p) > 1:
            # All-uppercase abbreviation — keep as-is
            titled.append(p)
        else:
            titled.append(p.capitalize())
    return " ".join(titled)


def classify(key: str, size: int) -> tuple[str, str | None, str] | None:
    """Return (group_id, subgroup_id_or_None, filename) or None to skip."""
    # Skip zero-size directory markers
    if size == 0 and key.endswith("/"):
        return None

    if "/" not in key:
        # Top-level file
        for pattern, group in TOP_LEVEL_RULES:
            if pattern.search(key):
                return (group, None, key)
        return ("misc", None, key)

    slash_idx = key.index("/")
    top_dir = key[:slash_idx]
    rest = key[slash_idx + 1:]

    if top_dir == WTX_MANUSCRIPT and "/" in rest:
        sub_slash = rest.index("/")
        sub_dir = rest[:sub_slash]
        filename = rest[sub_slash + 1:]
        return (top_dir, f"{top_dir}/{sub_dir}", filename)
    else:
        return (top_dir, None, rest)


def load_tsv(path: str) -> list[tuple[str, int, str]]:
    entries: list[tuple[str, int, str]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            key, size_str, modified = parts[0], parts[1], parts[2]
            try:
                size = int(size_str)
            except ValueError:
                size = 0
            entries.append((key, size, modified))
    return entries


def build_groups(entries: list[tuple[str, int, str]]) -> list[dict]:
    # group_id -> {"files": [...], "subgroups": {subgroup_id -> {"files": [...]}}}
    groups: dict[str, dict] = {}

    for key, size, modified in entries:
        result = classify(key, size)
        if result is None:
            continue
        group_id, subgroup_id, filename = result

        if group_id not in groups:
            groups[group_id] = {"files": [], "subgroups": {}}

        file_entry = {
            "key": key,
            "filename": filename,
            "size_bytes": size,
            "last_modified": modified,
        }

        if subgroup_id is not None:
            if subgroup_id not in groups[group_id]["subgroups"]:
                groups[group_id]["subgroups"][subgroup_id] = {"files": []}
            groups[group_id]["subgroups"][subgroup_id]["files"].append(file_entry)
        else:
            groups[group_id]["files"].append(file_entry)

    def agg(files: list[dict]) -> tuple[int, int, str]:
        """Returns (total_size, file_count, latest_modified)."""
        total = sum(f["size_bytes"] for f in files)
        latest = max((f["last_modified"] for f in files), default="")
        return total, len(files), latest

    result_groups = []
    for group_id in sorted(groups.keys()):
        g = groups[group_id]
        top_files = g["files"]
        top_total, top_count, top_latest = agg(top_files)

        subgroups_list = None
        if g["subgroups"]:
            subgroups_list = []
            for sub_id in sorted(g["subgroups"].keys()):
                sub_files = g["subgroups"][sub_id]["files"]
                sub_total, sub_count, sub_latest = agg(sub_files)
                sub_files_sorted = sorted(sub_files, key=lambda f: f["key"])
                subgroups_list.append({
                    "group_id": sub_id,
                    "display_name": make_display_name(sub_id),
                    "total_size_bytes": sub_total,
                    "file_count": sub_count,
                    "latest_modified": sub_latest,
                    "files": sub_files_sorted,
                })

        top_files_sorted = sorted(top_files, key=lambda f: f["key"])
        result_groups.append({
            "group_id": group_id,
            "display_name": make_display_name(group_id),
            "total_size_bytes": top_total,
            "file_count": top_count,
            "latest_modified": top_latest,
            "files": top_files_sorted,
            "subgroups": subgroups_list,
        })

    return result_groups


def collect_ids_and_names(groups: list[dict]) -> tuple[list[str], dict[str, str]]:
    ids: list[str] = []
    names: dict[str, str] = {}
    for g in groups:
        ids.append(g["group_id"])
        names[g["group_id"]] = g["display_name"]
        for sg in g.get("subgroups") or []:
            ids.append(sg["group_id"])
            names[sg["group_id"]] = sg["display_name"]
    return ids, names


def merge_status(dataset_ids: list[str], display_names: dict[str, str], csv_path: str) -> None:
    existing: dict[str, dict] = {}
    try:
        with open(csv_path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                existing[row["dataset_id"]] = dict(row)
    except FileNotFoundError:
        pass

    rows = []
    for gid in sorted(dataset_ids):
        if gid in existing:
            rows.append(existing[gid])
        else:
            rows.append({
                "dataset_id": gid,
                "display_name": display_names.get(gid, make_display_name(gid)),
                "downloaded": "",
                "converted_to_spatialdata": "",
                "uploaded_to_lamin": "",
                "notes": "",
            })

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STATUS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    added = len(set(dataset_ids) - set(existing.keys()))
    print(f"Status CSV: {len(rows)} total rows, {added} new")


def main() -> int:
    files_path = sys.argv[1] if len(sys.argv) > 1 else FILES_DEFAULT
    json_path = sys.argv[2] if len(sys.argv) > 2 else JSON_DEFAULT
    status_path = sys.argv[3] if len(sys.argv) > 3 else STATUS_DEFAULT

    entries = load_tsv(files_path)
    print(f"Loaded {len(entries)} entries from {files_path}")

    groups = build_groups(entries)
    print(f"Grouped into {len(groups)} top-level groups")

    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    output = {
        "last_updated": now,
        "bucket_url": BUCKET_URL,
        "groups": groups,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"Wrote {json_path}")

    all_ids, display_names = collect_ids_and_names(groups)
    merge_status(all_ids, display_names, status_path)
    print(f"Wrote {status_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
