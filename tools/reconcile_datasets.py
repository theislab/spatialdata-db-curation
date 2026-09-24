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
