#!/bin/bash

# ──────────────────────────────────────────────────────────────
# SLURM job directives
# ──────────────────────────────────────────────────────────────

#SBATCH -p compute        # Submit the job to the 'compute' partition
#SBATCH -n 12             # Request 12 CPU cores for the job
#SBATCH --mem=32G         # Allocate 32 GB of memory

# ──────────────────────────────────────────────────────────────
# Environment setup
# ──────────────────────────────────────────────────────────────

# Load the Conda environment manager
. "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh"

# ──────────────────────────────────────────────────────────────
# Run the checksum scoring script using 12 parallel workers
# ──────────────────────────────────────────────────────────────

python ./compute_all_scores -n 12

