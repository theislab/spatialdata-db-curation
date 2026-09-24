#!/usr/bin/env python3
"""Verify scripts/metadata/datasets_merged.csv adds nothing beyond registry.

Only the extra `Software` column is unique to the merged file; if every value
is already reflected in the registry row's software_name/software_version, the
merged file is redundant and safe to delete.
"""
from __future__ import annotations

import csv
import sys


def _load(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def extra_info(merged: list[dict[str, str]], registry: list[dict[str, str]]) -> list[str]:
    reg = {r.get("dataset_id", ""): r for r in registry}
    flagged = []
    for m in merged:
        sw = (m.get("Software") or "").strip()
        if not sw:
            continue
        r = reg.get(m.get("dataset_id", ""))
        name = (r.get("software_name") or "").strip() if r else ""
        version = (r.get("software_version") or "").strip() if r else ""
        # merged's Software is a combined string (e.g. "Space Ranger v2.1.0"),
        # while the registry splits it into software_name/software_version;
        # it's covered when both parts show up in that combined string.
        covered = bool(name) and bool(version) and name in sw and version in sw
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
