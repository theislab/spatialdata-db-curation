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
import os

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
    "Replicate": "Replicate",
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


def build_notes(scrape_row: dict[str, str]) -> str:
    parts = [f"{k}={scrape_row[k].strip()}" for k in NOTE_FIELDS
             if (scrape_row.get(k) or "").strip()]
    return "; ".join(parts)


def fold_new(
    registry: list[dict[str, str]], scrape: list[dict[str, str]]
) -> list[dict[str, str]]:
    if not registry:
        raise ValueError("registry must be non-empty to supply column schema")
    fieldnames = list(registry[0].keys())
    have = {(r.get("local_uid") or "").strip() for r in registry}
    out = [dict(r) for r in registry]
    seen: set[str] = set()
    for s in scrape:
        uid = (s.get("uid") or "").strip()
        if not uid or uid in have or uid in seen:
            continue
        seen.add(uid)
        row = {c: "" for c in fieldnames}
        for src, dst in SCRAPE_TO_REGISTRY.items():
            if dst in row:
                row[dst] = (s.get(src) or "").strip()
        if "manufacturer" in row:
            row["manufacturer"] = "10x Genomics"
        if "primary_source_type" in row:
            row["primary_source_type"] = "url"
        if "notes" in row:
            row["notes"] = build_notes(s)
        enriched = ensure_fingerprints_row(dict(row))
        for k in ("dataset_id", "primary_fingerprint"):
            if k in row:
                row[k] = enriched.get(k, row[k])
        out.append(row)
    return out


def load_keyspace_rows(path: str) -> list[dict[str, str]]:
    """Load the full ``uid;source;id`` keyspace rows (never mutated)."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        return [
            {
                "uid": (r.get("uid") or "").strip(),
                "source": (r.get("source") or "").strip(),
                "id": (r.get("id") or "").strip(),
            }
            for r in reader
        ]


def load_uid_keyspace(path: str) -> set[str]:
    return {r["uid"] for r in load_keyspace_rows(path) if r["uid"]}


def free_10x_uids(keyspace_rows: list[dict[str, str]], used: set[str]) -> list[str]:
    """Sorted 10x Genomics uids with no assigned id and not already `used`."""
    return sorted(
        r["uid"]
        for r in keyspace_rows
        if r.get("source") == "10x Genomics" and not (r.get("id") or "").strip()
        and r["uid"] not in used
    )


def mint_missing_uids(
    registry: list[dict[str, str]], keyspace_rows: list[dict[str, str]]
) -> list[dict[str, str]]:
    """Fill every empty ``local_uid`` with a fresh, never-reused 10x uid.

    Existing non-empty ``local_uid`` values are retained untouched. Minting
    is deterministic (uids are consumed in sorted order) and never reuses a
    uid already assigned elsewhere in the registry or minted earlier in this
    same call.
    """
    used = {(r.get("local_uid") or "").strip() for r in registry if (r.get("local_uid") or "").strip()}
    pool = iter(free_10x_uids(keyspace_rows, used))
    out: list[dict[str, str]] = []
    for row in registry:
        row = dict(row)
        if not (row.get("local_uid") or "").strip():
            try:
                new_uid = next(pool)
            except StopIteration:
                raise ValueError("free 10x uid pool exhausted while minting missing uids")
            row["local_uid"] = new_uid
        out.append(row)
    return out


def check_unique(registry: list[dict[str, str]]) -> list[str]:
    """Return offending ``local_uid`` values: duplicates, plus an "<empty>" sentinel
    for any row with a blank ``local_uid``."""
    seen: dict[str, int] = {}
    empty = 0
    for r in registry:
        uid = (r.get("local_uid") or "").strip()
        if not uid:
            empty += 1
            continue
        seen[uid] = seen.get(uid, 0) + 1
    offenders = sorted(uid for uid, count in seen.items() if count > 1)
    if empty:
        offenders.append(f"<empty> x{empty}")
    return offenders


def check_keyspace(registry: list[dict[str, str]], keyspace: set[str]) -> list[str]:
    bad = []
    for r in registry:
        uid = (r.get("local_uid") or "").strip()
        if uid and uid not in keyspace:
            bad.append(uid)
    return sorted(set(bad))


def reconcile(
    registry: list[dict[str, str]],
    scrape: list[dict[str, str]],
    keyspace_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    linked, _ = backfill_uids(registry, scrape)
    folded = fold_new(linked, scrape)
    minted = mint_missing_uids(folded, keyspace_rows)

    keyspace = {r["uid"] for r in keyspace_rows if r["uid"]}
    bad = check_keyspace(minted, keyspace)
    if bad:
        raise ValueError(f"UIDs not in keyspace registry/uids.csv: {bad}")

    offenders = check_unique(minted)
    if offenders:
        raise ValueError(f"local_uid not unique/complete after minting: {offenders}")

    # check_unique above already raised if any local_uid were empty, so every
    # row now has one — the report is always empty.
    unmatched: list[dict[str, str]] = []
    return minted, unmatched


def write_unmatched(path: str, rows: list[dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else ["dataset_id", "name", "primary_source", "Replicate"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="nonzero exit if a run would change outputs")
    args = ap.parse_args(argv)

    registry = load_registry(REGISTRY)
    scrape = load_scrape(SCRAPE)
    keyspace_rows = load_keyspace_rows(UIDS)
    new_registry, unmatched = reconcile(registry, scrape, keyspace_rows)

    if args.check:
        changed = new_registry != registry
        print("CHANGED" if changed else "OK")
        return 1 if changed else 0

    write_registry(REGISTRY, new_registry)
    write_unmatched(UNMATCHED, unmatched)
    print(f"registry rows: {len(new_registry)}  unmatched: {len(unmatched)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
