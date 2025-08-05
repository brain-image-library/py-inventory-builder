import pandas as pd
from dask import delayed, compute
from tqdm import tqdm
from dask.diagnostics import ProgressBar
import argparse
from datetime import datetime
import os
import sys

# ─────────────────────────────────────────────
# Argument parser setup
# ─────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Compute checksum coverage scores in parallel.")
parser.add_argument(
    "-n", "--number-of-workers",
    type=int,
    default=8,
    help="Number of parallel worker threads to use (default: 8)"
)
args = parser.parse_args()

# ─────────────────────────────────────────────
# Check if input file exists
# ─────────────────────────────────────────────
if not os.path.isfile("summary_metadata.tsv"):
    print("❌ File 'summary_metadata.tsv' not found in current directory. Exiting.")
    sys.exit(1)

# ─────────────────────────────────────────────
# Load the input TSV
# ─────────────────────────────────────────────
print("🔄 Loading summary_metadata.tsv...")
df = pd.read_csv("summary_metadata.tsv", sep="\t", low_memory=False)

# ─────────────────────────────────────────────
# Add 'last_update' column if missing
# ─────────────────────────────────────────────
if 'last_update' not in df.columns:
    print("➕ Adding missing column: last_update")
    df['last_update'] = pd.NaT  # Ensure datetime type
else:
    df['last_update'] = pd.to_datetime(df['last_update'], errors="coerce")  # Normalize format

# ─────────────────────────────────────────────
# Get today's date in YYYYMMDD format
# ─────────────────────────────────────────────
today = datetime.today().strftime("%Y%m%d")

# ─────────────────────────────────────────────
# Define delayed checksum coverage computation
# ─────────────────────────────────────────────
@delayed
def compute_coverage(index, dataset):
    try:
        temp_df = pd.read_csv(dataset['temp_file'], sep='\t', low_memory=False)
        return {
            "index": index,
            "md5_coverage": temp_df['md5'].notna().mean(),
            "sha256_coverage": temp_df['sha256'].notna().mean(),
            "xxh64_coverage": temp_df['xxh64'].notna().mean(),
            "b2sum_coverage": temp_df['b2sum'].notna().mean(),
            "updated": True
        }
    except Exception:
        return {
            "index": index,
            "md5_coverage": None,
            "sha256_coverage": None,
            "xxh64_coverage": None,
            "b2sum_coverage": None,
            "updated": False
        }

# ─────────────────────────────────────────────
# Generate parallel Dask tasks
# ─────────────────────────────────────────────
print(f"🚀 Submitting coverage computation tasks with {args.number_of_workers} workers...")
tasks = [compute_coverage(i, row) for i, row in tqdm(df.iterrows(), total=len(df), desc="Creating tasks")]

# ─────────────────────────────────────────────
# Execute tasks with progress bar
# ─────────────────────────────────────────────
print("⚙️  Executing tasks in parallel (Dask)...")
with ProgressBar():
    results = compute(*tasks, scheduler='threads', num_workers=args.number_of_workers)

# ─────────────────────────────────────────────
# Apply results to DataFrame
# ─────────────────────────────────────────────
print("📝 Updating DataFrame with computed values...")
for res in results:
    idx = res["index"]
    df.at[idx, "md5_coverage"] = res["md5_coverage"]
    df.at[idx, "sha256_coverage"] = res["sha256_coverage"]
    df.at[idx, "xxh64_coverage"] = res["xxh64_coverage"]
    df.at[idx, "b2sum_coverage"] = res["b2sum_coverage"]
    if res["updated"]:
        df.at[idx, "last_update"] = pd.to_datetime(today, format="%Y%m%d")

# ─────────────────────────────────────────────
# Ensure 'last_update' column remains datetime
# ─────────────────────────────────────────────
df["last_update"] = pd.to_datetime(df["last_update"], errors="coerce")

# ─────────────────────────────────────────────
# Compute average coverage score
# ─────────────────────────────────────────────
print("📊 Computing average coverage score...")
coverage_cols = ["md5_coverage", "b2sum_coverage", "sha256_coverage", "xxh64_coverage"]
df["score"] = df[coverage_cols].mean(axis=1)

# ─────────────────────────────────────────────
# Reorder columns: last_update first, score last
# ─────────────────────────────────────────────
print("📦 Reordering columns: 'last_update' first, 'score' last...")
middle_columns = [col for col in df.columns if col not in ["last_update", "score"]]
ordered_columns = ["last_update"] + middle_columns + ["score"]
df = df[ordered_columns]

# ─────────────────────────────────────────────
# Sort by score ascending
# ─────────────────────────────────────────────
print("📉 Sorting by 'score' in ascending order...")
df = df.sort_values(by="score", ascending=True)

# ─────────────────────────────────────────────
# Save updated TSV file
# ─────────────────────────────────────────────
print("💾 Saving updated summary_metadata.tsv")
df.to_csv("summary_metadata.tsv", sep="\t", index=False)

print("✅ Done.")
