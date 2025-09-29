#!/usr/bin/env python3
from __future__ import annotations

from collections import defaultdict
from canon import load_registry


def main(path: str) -> None:
    rows = load_registry(path)
    print("## Duplicate check by source fingerprint\n")
    if not rows:
        print("Registry is empty.")
        return
    idx = defaultdict(list)
    for r in rows:
        dsid = r.get("dataset_id", "")
        fps = [s for s in (r.get("fingerprints") or "").split("|") if s]
        pf = r.get("primary_fingerprint", "")
        for fp in fps + ([pf] if pf else []):
            if dsid:
                idx[fp].append(dsid)
    dups = {k: sorted(set(v)) for k, v in idx.items() if len(set(v)) > 1}
    if not dups:
        print("No duplicates found.\n")
        return
    print("The following fingerprints are referenced by multiple dataset_ids:\n")
    for fp, ids in sorted(dups.items()):
        id_list = ", ".join(ids)
        print(f"- `{fp}` → {id_list}")


if __name__ == "__main__":
    import sys
    main(sys.argv[1])