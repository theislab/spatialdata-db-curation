"""Generate the reserved SpatialData-DB UID keyspace.

Curation owns UID assignment; this reproduces the reserved keyspace stored in
``registry/uids.csv``. Each provider gets a 3-character base drawn from 31
characters (``a-z0-9`` minus the look-alikes ``l b o g q`` -> 31**3 = 29,791
ids) behind a 2-character prefix.

Run prints to stdout so it never clobbers ``registry/uids.csv`` (which also
holds assignments in the ``id`` column):

    python tools/mint_uids.py > /tmp/keyspace.csv
"""

import itertools
import string
import sys

import pandas as pd

CHARS = "".join(c for c in string.ascii_lowercase + string.digits if c not in "lbogq")

# prefix -> source label
SPACES = {
    "10": "10x Genomics",
    "vg": "vizgen",
    "ns": "nanostring",
    "xx": "misc",
}


def _ids(prefix: str, size: int = 3) -> list[str]:
    return [prefix + "".join(p) for p in itertools.product(CHARS, repeat=size)]


def mint() -> pd.DataFrame:
    """Return the full reserved keyspace as a ``uid;source;id`` frame (empty ids)."""
    frames = []
    for prefix, source in SPACES.items():
        df = pd.DataFrame({"uid": _ids(prefix)})
        df["source"] = source
        df["id"] = ""
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


if __name__ == "__main__":
    mint().to_csv(sys.stdout, sep=";", index=False)
