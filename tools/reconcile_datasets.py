#!/usr/bin/env python3
"""Reconcile the raw 10x scrape into the canonical dataset registry.

Reads sources/datasets_10x.csv (scrape input) + registry/datasets.csv
(canonical output) + registry/uids.csv (keyspace), links scrape UIDs into the
registry, folds in scrape-only datasets, and writes an unmatched report for
rows a machine cannot safely link. Reuses tools/canon.py.

    python tools/reconcile_datasets.py           # write registry + report
    python tools/reconcile_datasets.py --check   # nonzero exit if a run would change files
"""
from __future__ import annotations

import argparse
import csv

from canon import (
    canonical_source,
    ensure_fingerprints_row,
    fingerprint,
    load_registry,
    write_registry,
)

SCRAPE = "sources/datasets_10x.csv"
REGISTRY = "registry/datasets.csv"
UIDS = "registry/uids.csv"
UNMATCHED = "tools/reports/datasets_unmatched.csv"

SCRAPE_TO_REGISTRY = {
    "Datasets": "name",
    "Products": "product",
    "Software": "software_name",
    "Pipeline Version": "software_version",
    "dataset_link": "primary_source",
    "Publish Date": "release_date",
    "uid": "local_uid",
}
NOTE_FIELDS = [
    "Chemistry Version", "Subpipeline", "Species", "Disease State",
    "Anatomical entity", "organ", "tech", "Preservation Method",
    "Staining Method", "Biomaterial type", "10x Instrument(s)",
    "Feature Barcode", "Cells or nuclei", "Cell Nuclei count",
]


def load_scrape(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=";"))


def source_fp(url: str | None) -> str:
    c = canonical_source(url or "")
    return fingerprint(c) if c else ""


def scrape_key(row: dict[str, str]) -> tuple[str, str]:
    return (source_fp(row.get("dataset_link", "")), (row.get("Replicate") or "").strip())


def registry_key(row: dict[str, str]) -> tuple[str, str]:
    fp = (row.get("primary_fingerprint") or "").strip() or source_fp(row.get("primary_source", ""))
    return (fp, (row.get("Replicate") or "").strip())


def _scrape_uid_index(scrape: list[dict[str, str]]) -> dict[tuple[str, str], set[str]]:
    idx: dict[tuple[str, str], set[str]] = {}
    for r in scrape:
        fp, rep = scrape_key(r)
        uid = (r.get("uid") or "").strip()
        if not fp or not rep or not uid:
            continue
        idx.setdefault((fp, rep), set()).add(uid)
    return idx


def backfill_uids(
    registry: list[dict[str, str]], scrape: list[dict[str, str]]
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    idx = _scrape_uid_index(scrape)
    out: list[dict[str, str]] = []
    unmatched: list[dict[str, str]] = []
    for row in registry:
        row = dict(row)
        if (row.get("local_uid") or "").strip():
            out.append(row)
            continue
        fp, rep = registry_key(row)
        uids = idx.get((fp, rep), set()) if fp and rep else set()
        if len(uids) == 1:
            row["local_uid"] = next(iter(uids))
        else:
            unmatched.append(row)
        out.append(row)
    return out, unmatched
