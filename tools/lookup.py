#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from canon import load_registry, canonical_source, fingerprint, index_by_fingerprint


def main() -> int:
    p = argparse.ArgumentParser(description="Lookup dataset by DOI or URL.")
    p.add_argument("registry", help="Path to registry/datasets.csv")
    p.add_argument("source", help="DOI or URL to check")
    args = p.parse_args()

    c = canonical_source(args.source)
    if c is None:
        print("Input does not look like a DOI or resolvable URL.")
        return 2
    fp = fingerprint(c)
    rows = load_registry(args.registry)
    idx = index_by_fingerprint(rows)
    match = idx.get(fp, [])
    if match:
        uniq = sorted({m for m in match if m})
        print(f"FOUND: {uniq}")
        return 0
    print("NOT FOUND")
    return 1


if __name__ == "__main__":
    sys.exit(main())