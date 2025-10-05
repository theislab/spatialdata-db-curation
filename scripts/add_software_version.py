import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup

IN_CSV = "datasets_merged.csv"
OUT_CSV = "datasets_with_software.csv"
TIMEOUT = 20
SLEEP_BETWEEN = 0.8

RXES = [
    (re.compile(r"\bSpace\s*Ranger\s*(?:v(?:ersion)?\s*)?(\d+\.\d+\.\d+)\b", re.I), "Space Ranger"),
    (re.compile(r"analyzed using\s*Space\s*Ranger\s*(\d+\.\d+\.\d+)\b", re.I), "Space Ranger"),
    (re.compile(r"\bXenium\s*Onboard\s*Analysis\s*v(?:ersion\s*)?(\d+\.\d+\.\d+)\b", re.I), "Xenium Onboard Analysis"),
    (re.compile(r"on[- ]instrument analysis .*Xenium\s*Onboard\s*Analysis\s*v(\d+\.\d+\.\d+)", re.I), "Xenium Onboard Analysis"),
]

def fetch_html(url: str) -> str | None:
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.text
    except requests.RequestException:
        return None
    return None

def extract_version(html: str) -> tuple[str | None, str | None]:
    if not html:
        return (None, None)
    soup = BeautifulSoup(html, "html.parser")
    texts = " ".join(s.get_text(separator=" ", strip=True) for s in soup.find_all())
    corpus = texts + " " + html
    for rx, name in RXES:
        m = rx.search(corpus)
        if m:
            return (name, m.group(1))
    return (None, None)

df = pd.read_csv(IN_CSV)
soft_names, soft_versions = [], []

for _, row in df.iterrows():
    url = str(row.get("primary_source", "")).strip()
    if not url or url == "nan":
        soft_names.append(None); soft_versions.append(None)
        continue
    html = fetch_html(url)
    name, ver = extract_version(html)
    soft_names.append(name)
    soft_versions.append(ver)
    time.sleep(SLEEP_BETWEEN)

df["software_name"] = soft_names
df["software_version"] = soft_versions
df.to_csv(OUT_CSV, index=False)
print(f"Wrote {OUT_CSV} with {len(df)} rows.")
