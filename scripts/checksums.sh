#!/bin/bash

#SBATCH -n 16
#SBATCH --mem=32G
#SBATCH -p compute

# Load Conda environment
if [ "$USER" = "icaoberg" ] && [ -f "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh" ]; then
	. /bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh
	conda activate base
        cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f9 | grep "/bil/data/" | awk 'NR >= 9750 && NR <= 10407'  | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 32  --compress --update --remove-checkpoints
fi
