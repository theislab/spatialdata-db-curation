#!/usr/bin/env python3
"""Fetch the Bruker SMI public S3 bucket listing and write it to a TSV file.

Usage:
    python tools/fetch_bruker.py [output_path]

Output format (no header, tab-separated):
    key <TAB> size_bytes <TAB> last_modified
"""
from __future__ import annotations

import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

BUCKET_URL = "https://smi-public.objects.liquidweb.services/"
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def fetch_all_entries(bucket_url: str) -> list[tuple[str, str, str]]:
    """Return list of (key, size_bytes, last_modified) for all objects in bucket."""
    entries: list[tuple[str, str, str]] = []
    marker = ""

    while True:
        url = bucket_url
        if marker:
            url = bucket_url + "?" + urllib.parse.urlencode({"marker": marker})

        with urllib.request.urlopen(url) as resp:
            tree = ET.parse(resp)

        root = tree.getroot()

        for contents in root.findall(f"{NS}Contents"):
            key_el = contents.find(f"{NS}Key")
            size_el = contents.find(f"{NS}Size")
            mod_el = contents.find(f"{NS}LastModified")
            if key_el is None or size_el is None or mod_el is None:
                continue
            entries.append((key_el.text or "", size_el.text or "0", mod_el.text or ""))

        is_truncated = root.find(f"{NS}IsTruncated")
        if is_truncated is not None and is_truncated.text.lower() == "true" and entries:
            marker = entries[-1][0]
        else:
            break

    return entries


def main() -> int:
    out_path = sys.argv[1] if len(sys.argv) > 1 else "registry/bruker_files.txt"
    print(f"Fetching {BUCKET_URL} ...")
    entries = fetch_all_entries(BUCKET_URL)
    with open(out_path, "w", encoding="utf-8") as f:
        for key, size, modified in entries:
            f.write(f"{key}\t{size}\t{modified}\n")
    print(f"Wrote {len(entries)} entries to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
