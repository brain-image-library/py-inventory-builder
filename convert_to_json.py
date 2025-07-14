from pathlib import Path
import pandas as pd
from datetime import date

# Define the input and output file paths
tsv_path = Path("summary_metadata.tsv")
json_path = Path(f"today.json")

# Check if the TSV file exists
if tsv_path.exists():
    # Read the TSV file
    df = pd.read_csv(tsv_path, sep="\t")

    # Convert to JSON and save
    df.to_json(json_path, orient="records", indent=2)
    print(f"Converted '{tsv_path}' to '{json_path}'.")
else:
    print(f"'{tsv_path}' not found. No action taken.")

