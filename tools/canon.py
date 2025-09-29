#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import os
import re
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

TRACKING_PREFIXES = (
    "utm_",
    "mc_",
)
TRACKING_KEYS = {
    "gclid",
    "fbclid",
    "yclid",
    "ref",
    "ref_",
}


def canonicalize_doi(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    s = s.replace("https://doi.org/", "").replace("http://doi.org/", "")
    s = s.replace("doi:", "").strip()
    # DOIs are case insensitive; lower for stability
    s = s.lower()
    return s or None


def canonicalize_url(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    # Ensure scheme
    if not s.startswith("http://") and not s.startswith("https://"):
        s = "https://" + s
    u = urlparse(s)
    if not u.netloc:
        return None
    netloc = u.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    # Drop fragments
    fragment = ""
    # Clean query: drop tracking params
    clean_q = []
    for k, v in parse_qsl(u.query, keep_blank_values=True):
        k_lower = k.lower()
        if k_lower in TRACKING_KEYS:
            continue
        if any(k_lower.startswith(pref) for pref in TRACKING_PREFIXES):
            continue
        clean_q.append((k, v))
    query = urlencode(sorted(clean_q))
    # Normalize path: collapse multiple slashes, strip trailing slash
    path = re.sub(r"/+", "/", u.path)
    if path.endswith("/") and path != "/":
        path = path[:-1]
    # Force https
    scheme = "https"
    normalized = urlunparse((scheme, netloc, path, "", query, fragment))
    return normalized


def is_doi_or_url(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    if s.startswith("http://") or s.startswith("https://"):
        return True
    if s.lower().startswith("doi:") or s.lower().startswith("10."):
        return True
    if "." in s and "/" in s:
        return True
    return False


def canonical_source(raw: str) -> str | None:
    d = canonicalize_doi(raw)
    if d is not None:
        return f"doi:{d}"
    u = canonicalize_url(raw)
    if u is not None:
        return u
    return None


def fingerprint(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


@dataclass
class Entry:
    dataset_id: str
    name: str
    primary_source: str
    primary_fingerprint: str
    all_sources: list[str]
    fingerprints: list[str]


def load_registry(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_registry(path: str, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise ValueError("No rows to write")
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def ensure_fingerprints_row(row: dict[str, str]) -> dict[str, str]:
    sources_raw = [s.strip() for s in (row.get("all_sources") or "").split("|") if s.strip()]
    if not sources_raw:
        primary = row.get("primary_source") or ""
        if primary.strip():
            sources_raw = [primary.strip()]
    canonicals: list[str] = []
    for s in sources_raw:
        c = canonical_source(s)
        if c is not None:
            canonicals.append(c)
    fps = [fingerprint(c) for c in canonicals]
    primary_c = canonical_source(row.get("primary_source", ""))
    primary_fp = fingerprint(primary_c) if primary_c else ""
    row["primary_fingerprint"] = primary_fp
    row["all_sources"] = "|".join(sources_raw)
    row["fingerprints"] = "|".join(fps)
    # Derive dataset_id if missing
    if not row.get("dataset_id"):
        if primary_fp:
            row["dataset_id"] = f"ds_{primary_fp}"
    return row


def index_by_fingerprint(rows: Iterable[dict[str, str]]) -> dict[str, list[str]]:
    idx: dict[str, list[str]] = {}
    for r in rows:
        fps = [s.strip() for s in (r.get("fingerprints") or "").split("|") if s.strip()]
        for fp in fps:
            idx.setdefault(fp, []).append(r.get("dataset_id", ""))
        pf = r.get("primary_fingerprint", "").strip()
        if pf:
            idx.setdefault(pf, []).append(r.get("dataset_id", ""))
    return idx
