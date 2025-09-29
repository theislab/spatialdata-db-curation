#!/usr/bin/env python3
from __future__ import annotations

import sys
from collections import defaultdict
from canon import load_registry, ensure_fingerprints_row


REQUIRED_COLUMNS = {
    "dataset_id",
    "name",
    "primary_source",
    "primary_fingerprint",
    "all_sources",
    "fingerprints",
}


def main(path: str) -> int:
    rows = load_registry(path)
    if not rows:
        print("ERROR: registry is empty")
        return 2
    missing_cols = REQUIRED_COLUMNS - set(rows[0].keys())
    if missing_cols:
        print(f"ERROR: missing columns: {sorted(missing_cols)}")
        return 2
    # Normalize rows in memory (does not write back; CI only)
    norm = [ensure_fingerprints_row(dict(r)) for r in rows]
    seen = defaultdict(list)
    for r in norm:
        for fp in [s for s in r.get("fingerprints", "").split("|") if s]:
            seen[fp].append(r["dataset_id"])
        pf = r.get("primary_fingerprint", "")
        if pf:
            seen[pf].append(r["dataset_id"])
    dup = {k: v for k, v in seen.items() if len(set(v)) > 1}
    if dup:
        print("WARNING: duplicate fingerprints spanning multiple dataset_ids detected:")
        for fp, ids in dup.items():
            ids_sorted = sorted(set(ids))
            print(f"  fp={fp} ids={ids_sorted}")
        # Do not fail CI here; return success but nonzero if you want strict mode
        return 0
    print("OK: no cross‑ID duplicates detected")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))