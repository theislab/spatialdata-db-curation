import pandas as pd
import numpy as np

datasets_registry = pd.read_csv("datasets.csv").drop(columns=["lamin_link"], errors="ignore")
on_cluster = pd.read_csv("on_cluster.csv", sep=";")[["dataset_link","uid"]].rename(columns={"uid":"local_uid"})
uploaded_datasets = pd.read_csv("uploaded_datasets.csv").drop(columns=["local_uid"], errors="ignore")

datasets_registry = pd.merge(datasets_registry, on_cluster, left_on="primary_source", right_on="dataset_link", how="left")
merged_datasets = pd.merge(datasets_registry, uploaded_datasets, left_on="primary_source", right_on="Dataset Url", how="left")

columns_to_keep = list(dict.fromkeys(datasets_registry.columns.tolist() + ["lamin_link","uid","created_at","description"]))
merged_datasets = merged_datasets[columns_to_keep]

lamin_nonempty = merged_datasets["lamin_link"].astype(str).str.strip().ne("").fillna(False)
local_nonempty = merged_datasets["local_uid"].astype(str).str.strip().ne("").fillna(False)
merged_datasets["status"] = np.where(lamin_nonempty, "uploaded", np.where(local_nonempty, "downloaded", "todo"))

merged_datasets.to_csv("datasets_merged.csv", index=False)
