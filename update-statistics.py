from pandarallel import pandarallel

pandarallel.initialize(
    progress_bar=True
)  # Set to False if you don’t want tqdm-style progress
import pandas as pd
from pathlib import Path
from tqdm import tqdm


def get_temp_file(directory):
    """
    Generates a temporary file path based on a directory path.

    Args:
        directory (str): The directory path.

    Returns:
        str or None: The temporary file path, or None if the file does not exist.
    """
    if directory[-1] == "/":
        directory = directory[:-1]
    file = directory.replace("/", "_")
    output_filename = f"/bil/pscstaff/icaoberg/bil-inventory/.data/{file}.tsv"

    if Path(output_filename).exists():
        return output_filename
    else:
        return None


# Try importing humanize; install if not available
try:
    import humanize
except ImportError:
    import subprocess

    subprocess.check_call(["pip", "install", "humanize"])
    import humanize

# -------------------------------
# Load and validate input file
# -------------------------------
summary_file = Path("summary_metadata.tsv")
if not summary_file.exists():
    raise FileNotFoundError(f"{summary_file} not found.")

df = pd.read_csv(summary_file, sep="\t")

# -------------------------------
# Rewrite temp_file if it starts with .data/
# -------------------------------
df["temp_file"] = df["bildirectory"].parallel_apply(get_temp_file)


# Ensure required columns are present
required_columns = ["temp_file", "bildid"]
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"'{col}' column not found in summary_metadata.tsv")

# -------------------------------
# Initialize missing columns
# -------------------------------
for col in ["number_of_files", "size", "md5_coverage"]:
    if col not in df.columns:
        df[col] = pd.NA

# Ensure correct dtypes
df["number_of_files"] = pd.to_numeric(df["number_of_files"], errors="coerce").astype(
    "Int64"
)
df["size"] = pd.to_numeric(df["size"], errors="coerce").astype("Int64")
df["md5_coverage"] = pd.to_numeric(df["md5_coverage"], errors="coerce").astype(
    "Float64"
)

if not "xxh64_coverage" in df.keys():
    df["xxh64_coverage"] = None

if not "b2sum_coverage" in df.keys():
    df["b2sum_coverage"] = None

# -------------------------------
# Define constants
# -------------------------------
save_interval = 250
output_path = Path("summary_metadata.tsv")

# -------------------------------
# Process each row
# -------------------------------
for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing metadata"):
    already_done = not pd.isna(row["number_of_files"]) and not pd.isna(row["size"])
    if already_done:
        continue

    temp_file_val = row["temp_file"]
    if isinstance(temp_file_val, str) and temp_file_val.strip():
        temp_path = Path(temp_file_val)
    else:
        print(f"Invalid or missing temp_file for row {idx}")

    if temp_path.exists() and temp_path.is_file():
        try:
            temp_df = pd.read_csv(temp_path, sep="\t", low_memory=False)

            if pd.isna(row["number_of_files"]):
                df.at[idx, "number_of_files"] = int(len(temp_df))

            if pd.isna(row["size"]) and "size" in temp_df.columns:
                total_size = temp_df["size"].sum(min_count=1)
                df.at[idx, "size"] = int(total_size) if pd.notna(total_size) else pd.NA

            # md5
            if pd.isna(row["md5_coverage"]):
                if "md5" in temp_df.columns:
                    total = len(temp_df)
                    non_empty = temp_df["md5"].notna().sum()
                    coverage = (non_empty / total) * 100 if total > 0 else pd.NA
                    df.at[idx, "md5_coverage"] = round(coverage, 2)
                else:
                    print(
                        f"'md5' column missing in {temp_path}, skipping md5_coverage calculation."
                    )
                    df.at[idx, "md5_coverage"] = None

            # sha256
            if pd.isna(row["sha256_coverage"]):
                if "md5" in temp_df.columns:
                    total = len(temp_df)
                    non_empty = temp_df["sha256"].notna().sum()
                    coverage = (non_empty / total) * 100 if total > 0 else pd.NA
                    df.at[idx, "sha256_coverage"] = round(coverage, 2)
                else:
                    print(
                        f"'sha256' column missing in {temp_path}, skipping sha256_coverage calculation."
                    )
                    df.at[idx, "sha256_coverage"] = None

            # xxh64
            if pd.isna(row["xxh64_coverage"]):
                if "xxh64" in temp_df.columns:
                    total = len(temp_df)
                    non_empty = temp_df["xxh64"].notna().sum()
                    coverage = (non_empty / total) * 100 if total > 0 else pd.NA
                    df.at[idx, "xxh64_coverage"] = round(coverage, 2)
                else:
                    print(
                        f"'xxh64' column missing in {temp_path}, skipping xxh64_coverage calculation."
                    )
                    df.at[idx, "xxh64_coverage"] = None

            # b2sum
            if pd.isna(row["b2sum_coverage"]):
                if "b2sum" in temp_df.columns:
                    total = len(temp_df)
                    non_empty = temp_df["b2sum"].notna().sum()
                    coverage = (non_empty / total) * 100 if total > 0 else pd.NA
                    df.at[idx, "b2sum_coverage"] = round(coverage, 2)
                else:
                    print(
                        f"'b2sum' column missing in {temp_path}, skipping b2sum_coverage calculation."
                    )
                    df.at[idx, "b2sum_coverage"] = None

            # frequencies
            import json as _json
            df.at[idx, "frequencies"] = _json.dumps(
                temp_df["extension"]
                .dropna()
                .value_counts()
                .sort_values(ascending=False)
                .to_dict()
            )

            # filetype
            df.at[idx, "file_types"] = _json.dumps(
                temp_df["filetype"]
                .dropna()
                .value_counts()
                .sort_values(ascending=False)
                .to_dict()
            )

            # mime-types
            df.at[idx, "mime-types"] = _json.dumps(
                temp_df["mime-type"]
                .dropna()
                .value_counts()
                .sort_values(ascending=False)
                .to_dict()
            )

        except Exception as e:
            print(f"Error reading {temp_path}: {e}")
    else:
        print(f"File does not exist: {temp_path}")

    if (idx + 1) % save_interval == 0:
        df.to_csv(output_path, sep="\t", index=False)
        print(f"Saved progress at row {idx + 1} to '{output_path}'")

# -------------------------------
# Compute pretty_size using humanize
# -------------------------------
df["pretty_size"] = df["size"].apply(
    lambda x: humanize.naturalsize(x, binary=True) if pd.notna(x) else pd.NA
)

# -------------------------------
# Compute json_file paths if they exist
# -------------------------------
df["json_file"] = df["bildid"].apply(
    lambda b: (
        f"/bil/pscstaff/icaoberg/bil-inventory/json/{b}.json"
        if Path(f"/bil/pscstaff/icaoberg/bil-inventory/json/{b}.json").exists()
        else pd.NA
    )
)

# -------------------------------
# Final write to disk
# -------------------------------
df.to_csv(output_path, sep="\t", index=False)
print(f"Final data written to '{output_path}'")
