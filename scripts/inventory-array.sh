#!/bin/bash
#SBATCH -p compute
#SBATCH -n 12
#SBATCH --mem=128Gb
#SBATCH --array=1-500  # Update this range based on actual line count in dirs.txt
#SBATCH -o output/%A_%a.out
#SBATCH -e output/%A_%a.err

# Create output directory if it doesn't exist
mkdir -p output

# Get the dataset directory for this array task
DIRECTORY=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" temp.tsv | cut -d$'\t' -f 9)

# Run your script on this specific dataset
python ./manifest-builder.py -d "$DIRECTORY" -n 12 --compress --update --remove-checkpoints
