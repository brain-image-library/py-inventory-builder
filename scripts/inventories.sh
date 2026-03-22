#!/bin/bash

#SBATCH --job-name=inventories              # Job name
#SBATCH --output=inventories_%A_%a.out      # Standard output and error log
#SBATCH --error=inventories_%A_%a.err       # Error log
#SBATCH --array=1-10
#SBATCH --ntasks=1                          # Run a single task
#SBATCH --cpus-per-task=8                   # Number of CPU cores per task
#SBATCH --mem=32GB                          # Memory per node

set -x
cat temp.tsv | grep -v bildirectory | cut -d$'\t' -f9 | grep "/bil/data/" | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 16  --compress --update  --remove-checkpoints
