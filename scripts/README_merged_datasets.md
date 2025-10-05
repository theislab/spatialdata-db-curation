# Creating Merged Datasets

This script combines all the steps to create `datasets_merged.csv` into one unified Python script.

## Examples

```bash
# Full process with all intermediate files
python create_merged_datasets.py --save-intermediate

# Custom input/output
python create_merged_datasets.py --input my_datasets.csv --output my_merged.csv

# Basic usage - just create the final merged file
python create_merged_datasets.py --skip-softwar --skip-lamin
```
