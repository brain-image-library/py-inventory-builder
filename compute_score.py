import pandas as pd

# Load the TSV file
df = pd.read_csv("summary_metadata.tsv", sep="\t", low_memory=False)

# Compute the average coverage score
coverage_cols = ["md5_coverage", "b2sum_coverage", "sha256_coverage", "xxh64_coverage"]
df["score"] = df[coverage_cols].mean(axis=1)

# Sort by the new score column
df = df.sort_values(by="score", ascending=True)

# Save back to the original file
df.to_csv("summary_metadata.tsv", sep="\t", index=False)

