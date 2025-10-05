#!/usr/bin/env python3
"""
Get and match uploaded datasets from LaminDB with registry datasets.
"""

import re
import time
import argparse
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from typing import Optional, Tuple

try:
    import lamindb as ln
    LAMINDB_AVAILABLE = True
except ImportError:
    LAMINDB_AVAILABLE = False
    print("Warning: lamindb not available. LaminDB integration will be skipped.")

TIMEOUT = 20
SLEEP_BETWEEN = 0.8
SOFTWARE_PATTERNS = [
    (re.compile(r"\bSpace\s*Ranger\s*(?:v(?:ersion)?\s*)?(\d+\.\d+\.\d+)\b", re.I), "Space Ranger"),
    (re.compile(r"analyzed using\s*Space\s*Ranger\s*(\d+\.\d+\.\d+)\b", re.I), "Space Ranger"),
    (re.compile(r"\bXenium\s*Onboard\s*Analysis\s*v(?:ersion\s*)?(\d+\.\d+\.\d+)\b", re.I), "Xenium Onboard Analysis"),
    (re.compile(r"on[- ]instrument analysis .*Xenium\s*Onboard\s*Analysis\s*v(\d+\.\d+\.\d+)", re.I), "Xenium Onboard Analysis"),
]


def fetch_html(url: str) -> Optional[str]:
    """Fetch HTML content from URL with error handling."""
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.text
    except requests.RequestException:
        return None
    return None


def extract_software_version(html: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract software name and version from HTML content."""
    if not html:
        return (None, None)
    
    soup = BeautifulSoup(html, "html.parser")
    texts = " ".join(s.get_text(separator=" ", strip=True) for s in soup.find_all())
    corpus = texts + " " + html
    
    for pattern, name in SOFTWARE_PATTERNS:
        match = pattern.search(corpus)
        if match:
            return (name, match.group(1))
    
    return (None, None)


def add_software_versions(df: pd.DataFrame, save_intermediate: bool = False) -> pd.DataFrame:
    """Add software name and version information to datasets."""
    print("Adding software version information...")
    
    soft_names, soft_versions = [], []
    
    for idx, row in df.iterrows():
        url = str(row.get("primary_source", "")).strip()
        if not url or url == "nan":
            soft_names.append(None)
            soft_versions.append(None)
            continue
            
        html = fetch_html(url)
        name, version = extract_software_version(html)
        soft_names.append(name)
        soft_versions.append(version)
        
        if idx % 10 == 0:
            print(f"Processed {idx + 1}/{len(df)} datasets...")
        
        time.sleep(SLEEP_BETWEEN)
    
    df["software_name"] = soft_names
    df["software_version"] = soft_versions
    
    if save_intermediate:
        df.to_csv("datasets_with_software.csv", index=False)
        print(f"Saved intermediate file: datasets_with_software.csv")
    
    return df


def get_uploaded_datasets(save_intermediate: bool = False) -> pd.DataFrame:
    """Fetch uploaded datasets from LaminDB and merge with metadata."""
    print("Fetching uploaded datasets from LaminDB...")
    
    if not LAMINDB_AVAILABLE:
        print("Warning: LaminDB not available. Using empty dataset.")
        return pd.DataFrame()
    
    artifacts = ln.Artifact.df(limit=None, features=True, include=['description', 'created_at'])
    artifacts = artifacts[artifacts['key'].astype(str).str.startswith("10")]
    artifacts['key'] = artifacts['key'].apply(lambda x: x.split(".")[0])
    dataset_status = artifacts[['uid', 'key', 'created_at', 'description']].rename(columns={'uid': 'lamin_link', 'key': 'local_uid'})
    dataset_status.columns = dataset_status.columns.map(lambda x: x.replace(' ', '_')).str.lower()
    
    try:
        visium = pd.read_csv("metadata/visium_20250606.csv", sep=';')
        xenium = pd.read_csv("metadata/xenium_20250606.csv", sep=';')
    except FileNotFoundError as e:
        print(f"Error loading metadata files: {e}")
        return pd.DataFrame()
    
    result = pd.concat([visium, xenium], axis=0, ignore_index=True)
    result = result.merge(dataset_status, left_on='uid', right_on='local_uid', how='left')
    result['Dataset Url'] = result['Dataset Url'].apply(lambda x: x.replace('www.', ''))
    
    if save_intermediate:
        result.to_csv("uploaded_datasets.csv", index=False)
        print(f"Saved intermediate file: uploaded_datasets.csv")
        
        duplicate_urls = result[result.duplicated(subset=['Dataset Url'], keep=False)]['Dataset Url'].unique()
        duplicates_df = result[result['Dataset Url'].isin(duplicate_urls)][
            ['Dataset Url', 'Replicate', 'local_uid', 'Software', 'lamin_link']
        ].sort_values('Dataset Url')
        
        duplicates_df.to_csv("duplicate_datasets.csv", index=False)
        print(f"Found {len(duplicate_urls)} datasets with duplicates")
        print(f"Saved duplicate report: duplicate_datasets.csv")
    
    return result


def merge_datasets(registry_df: pd.DataFrame, uploaded_df: pd.DataFrame, save_intermediate: bool = False) -> pd.DataFrame:
    """Merge registry datasets with uploaded datasets."""
    print("Merging datasets...")
    
    registry_clean = registry_df.drop(columns=["lamin_link"], errors="ignore")
    
    print(registry_clean.columns.tolist())
    merged_datasets = pd.merge(registry_clean, uploaded_df, left_on="primary_source", right_on="Dataset Url", how="left")
    columns_to_keep = list(dict.fromkeys(registry_clean.columns.tolist() + [
        "local_uid", "lamin_link", "created_at", "description", "Replicate", "Software"
    ]))
    print(merged_datasets.columns.tolist())
    merged_datasets = merged_datasets[columns_to_keep]
    
    lamin_ends_with_0000 = merged_datasets["lamin_link"].notna() & (
        merged_datasets["lamin_link"].astype(str).str.endswith("0000")
    )
    merged_datasets = merged_datasets[lamin_ends_with_0000 | merged_datasets["lamin_link"].isna()]
    
    lamin_nonempty = (
        merged_datasets["lamin_link"].notna() & 
        (merged_datasets["lamin_link"].astype(str).str.strip() != "") & 
        (merged_datasets["lamin_link"].astype(str) != "nan")
    )
    merged_datasets["status"] = np.where(lamin_nonempty, "uploaded", "todo")
    
    if save_intermediate:
        merged_datasets.to_csv("datasets_merged.csv", index=False)
        print(f"Saved intermediate file: datasets_merged.csv")
    
    return merged_datasets


def main():
    """Main function to orchestrate the entire process."""
    parser = argparse.ArgumentParser(description="Create merged datasets CSV")
    parser.add_argument("--input", default="./metadata/scraped_datasets.csv", 
                       help="Input datasets CSV file")
    parser.add_argument("--output", default="./metadata/datasets_merged.csv", 
                       help="Output merged datasets CSV file")
    parser.add_argument("--save-intermediate", action="store_true", 
                       help="Save intermediate CSV files")
    parser.add_argument("--skip-software", action="store_true", 
                       help="Skip adding software version information")
    parser.add_argument("--skip-lamin", action="store_true", 
                       help="Skip fetching uploaded datasets from LaminDB")
    
    args = parser.parse_args()
    
    print("Starting dataset merging process...")
    print(f"Input: {args.input}")
    print(f"Output: {args.output}")
    print(f"Save intermediate files: {args.save_intermediate}")
    print(f"Skip software versions: {args.skip_software}")
    print(f"Skip LaminDB: {args.skip_lamin}")
    
    print("\n1. Loading base dataset...")
    try:
        registry_df = pd.read_csv(args.input)
        print(f"Loaded {len(registry_df)} datasets from {args.input}")
    except FileNotFoundError:
        print(f"Error: Could not find input file {args.input}")
        return 1
    
    if not args.skip_software:
        print("\n2. Adding software version information...")
        registry_df = add_software_versions(registry_df, args.save_intermediate)
    else:
        print("\n2. Skipping software version information...")
        registry_df = pd.read_csv('./datasets_with_software.csv')
    
    if not args.skip_lamin:
        print("\n3. Fetching uploaded datasets from LaminDB...")
        uploaded_df = get_uploaded_datasets(args.save_intermediate)
    else:
        print("\n3. Skipping LaminDB fetch...")
        uploaded_df = pd.read_csv('./uploaded_datasets.csv')
    
    print("\n4. Merging datasets...")
    final_df = merge_datasets(registry_df, uploaded_df, args.save_intermediate)
    
    print(f"\n5. Saving final result to {args.output}...")
    final_df.to_csv(args.output, index=False)
    
    print(f"\nSummary:")
    print(f"- Total datasets: {len(final_df)}")
    print(f"- Uploaded: {len(final_df[final_df['status'] == 'uploaded'])}")
    print(f"- Todo: {len(final_df[final_df['status'] == 'todo'])}")
    
    return 0


if __name__ == "__main__":
    exit(main())
