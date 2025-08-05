import os
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook

# Define file paths
tsv_path = "summary_metadata.tsv"
excel_path = "daily_summaries.xlsx"

# Generate timestamp for sheet name
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Load TSV into DataFrame
df = pd.read_csv(tsv_path, sep="\t")

# Check if Excel file exists and append accordingly
if os.path.exists(excel_path):
    # Open existing workbook and append
    with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='new') as writer:
        df.to_excel(writer, sheet_name=timestamp, index=False)
else:
    # Create new workbook and add sheet
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=timestamp, index=False)

