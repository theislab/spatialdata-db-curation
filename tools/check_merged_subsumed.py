#!/usr/bin/env python3
"""Verify scripts/metadata/datasets_merged.csv adds nothing beyond registry.

Only the extra `Software` column is unique to the merged file; if every value
is already reflected in the registry row's software_name/software_version, the
merged file is redundant and safe to delete.
"""
from __future__ import annotations

import csv


def _load(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _covers(row: dict[str, str], sw: str) -> bool:
    # merged's Software is a combined string (e.g. "Space Ranger v2.1.0"),
    # while the registry splits it into software_name/software_version;
    # it's covered when both parts show up in that combined string.
    name = (row.get("software_name") or "").strip()
    version = (row.get("software_version") or "").strip()
    return bool(name) and bool(version) and name in sw and version in sw


def extra_info(merged: list[dict[str, str]], registry: list[dict[str, str]]) -> list[str]:
    # dataset_id is NOT unique in the registry (many datasets share a source
    # URL); a last-wins {dataset_id: row} dict can silently compare a merged
    # row against the wrong registry row and mask a real discrepancy. Join on
    # the unique local_uid when the merged row carries one; otherwise fall
    # back to the full group of registry rows sharing that dataset_id and
    # require every row in the group to cover it (conservative: an ambiguous
    # or empty group is never silently treated as covered).
    reg_by_uid: dict[str, dict[str, str]] = {}
    reg_by_dataset_id: dict[str, list[dict[str, str]]] = {}
    for r in registry:
        uid = (r.get("local_uid") or "").strip()
        if uid:
            reg_by_uid[uid] = r
        reg_by_dataset_id.setdefault(r.get("dataset_id", ""), []).append(r)

    flagged = []
    for m in merged:
        sw = (m.get("Software") or "").strip()
        if not sw:
            continue
        uid = (m.get("local_uid") or "").strip()
        if uid:
            r = reg_by_uid.get(uid)
            covered = r is not None and _covers(r, sw)
        else:
            rows = reg_by_dataset_id.get(m.get("dataset_id", ""), [])
            covered = bool(rows) and all(_covers(r, sw) for r in rows)
        if not covered:
            flagged.append(m.get("dataset_id", ""))
    return flagged


def main() -> int:
    merged = _load("scripts/metadata/datasets_merged.csv")
    registry = _load("registry/datasets.csv")
    flagged = extra_info(merged, registry)
    if flagged:
        print(f"NOT subsumed; Software uncovered for: {sorted(set(flagged))}")
        return 1
    print("OK: datasets_merged.csv is subsumed by registry; safe to delete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
