# Contributing new datasets

1. Before adding, run a lookup to see if the source already exists:

   ```bash
   python tools/lookup.py registry/datasets.csv "https://manufacturer.com/product/xyz?utm_source=abc"
   python tools/lookup.py registry/datasets.csv "10.1038/s41586-020-03167-3"
````

2. If not found, add a new row to `registry/datasets.csv`:

   * Fill `name`, `primary_source`, `all_sources` (pipe‑separated if multiple).
   * Leave `dataset_id`, `primary_fingerprint`, `fingerprints` empty; CI or maintainers can backfill, or run `python -c "from tools.canon import load_registry, write_registry, ensure_fingerprints_row; import csv; rows=load_registry('registry/datasets.csv'); rows=[ensure_fingerprints_row(r) for r in rows]; write_registry('registry/datasets.csv', rows)"`.

3. Open a Pull Request. The bot will comment if your addition collides with existing entries.

4. Keep edits focused: one PR per dataset is ideal.

## Notes

- Fingerprints are deterministic on canonical DOI or URL. If two curators add the same source in different forms, the fingerprints collide and you get a clear match.
- The static UI only needs raw CSV. Serve `web/` via GitHub Pages, or open `web/index.html` locally; it fetches `../registry/datasets.csv`.
- If you later prefer YAML submissions in `entries/`, add a small transformer that builds the CSV during CI before checks.
