import lamindb as ln
import pandas as pd

artifacts = ln.Artifact.df(limit=None, features=True, include=['description', 'created_at'])
artifacts_date = ln.Artifact.df(limit=None)
artifacts = artifacts[artifacts['key'].astype(str).str.startswith("10")]
artifacts['key'] = artifacts['key'].apply(lambda x: x.split(".")[0])
dataset_status = artifacts[['uid', 'key', 'created_at', 'description']].rename(columns={'uid': 'lamin_link', 'key': 'local_uid'})
dataset_status.columns = dataset_status.columns.map(lambda x: x.replace(' ', '_')).str.lower()

visium = pd.read_csv("visium_20250606.csv", sep=';')
xenium = pd.read_csv("xenium_20250606.csv", sep=';')

result = pd.concat([visium, xenium], axis=0, ignore_index=True)
result = result.merge(dataset_status, left_on='uid', right_on='local_uid', how='left')
result['Dataset Url'] = result['Dataset Url'].apply(lambda x: x.replace('www.', ''))
result.to_csv("uploaded_datasets.csv", index=False)

duplicate_urls = result[result.duplicated(subset=['Dataset Url'], keep=False)]['Dataset Url'].unique()
duplicates_df = result[result['Dataset Url'].isin(duplicate_urls)][
    ['Dataset Url', 'Replicate', 'local_uid', 'Software', 'lamin_link']
].sort_values('Dataset Url')

duplicates_df.to_csv("duplicate_datasets.csv", index=False)
print(f"Found {len(duplicate_urls)} datasets with duplicates")
print(f"Wrote {len(duplicates_df)} total duplicate entries to duplicate_datasets.csv")

