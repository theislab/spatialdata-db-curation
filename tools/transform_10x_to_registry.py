#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from typing import List, Dict

from canon import canonical_source, fingerprint
from datetime import date


def map_row(src: Dict[str, str]) -> Dict[str, str]:
    # Source columns expected: dataset, product, species, sample_type, url
    name = (src.get("dataset") or "").strip()
    product = (src.get("product") or "").strip()
    species = (src.get("species") or "").strip()
    sample_type = (src.get("sample_type") or "").strip()
    url = (src.get("url") or "").strip()

    primary_source = canonical_source(url) or ""
    row = {
        "dataset_id": "",  # will be filled from fingerprint
        "name": name,
        "short_description": f"{product} | {species} | {sample_type}".strip(" |"),
        "primary_source_type": ("url" if primary_source.startswith("http") else ("doi" if primary_source.startswith("doi:") else "")),
        "primary_source": primary_source,
        "primary_fingerprint": "",  # will be set below
        "doi": "",
        "pmid": "",
        "manufacturer": "10x Genomics",
        "product": product,
        "release_date": "",
        "tags": ",".join([x for x in [species, sample_type] if x]),
        "curator": "[@timtreis](https://github.com/timtreis)",
        "last_updated": date.today().isoformat(),
        "notes": "",
    }
    primary_c = primary_source
    if primary_c:
        row["primary_fingerprint"] = fingerprint(primary_c)
        if not row.get("dataset_id"):
            row["dataset_id"] = f"ds_{row['primary_fingerprint']}"
    return row


def transform(input_csv: str, output_csv: str) -> None:
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        out_rows: List[Dict[str, str]] = []
        for src in reader:
            out_rows.append(map_row(src))

    # Deduplicate by dataset_id
    seen = set()
    deduped: List[Dict[str, str]] = []
    for r in out_rows:
        did = r.get("dataset_id", "")
        if did and did in seen:
            continue
        if did:
            seen.add(did)
        deduped.append(r)

    # Write
    fieldnames = [
        "dataset_id",
        "name",
        "short_description",
        "primary_source_type",
        "primary_source",
        "primary_fingerprint",
        "doi",
        "pmid",
        "manufacturer",
        "product",
        "release_date",
        "tags",
        "curator",
        "last_updated",
        "notes",
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in deduped:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def main() -> None:
    ap = argparse.ArgumentParser(description="Transform 10x scraped CSV to registry format")
    ap.add_argument("input_csv", nargs="?", default=os.path.join("registry", "20250928_10x_scrape.csv"))
    ap.add_argument("output_csv", nargs="?", default=os.path.join("registry", "datasets.csv"))
    args = ap.parse_args()
    transform(args.input_csv, args.output_csv)


if __name__ == "__main__":
    main()


