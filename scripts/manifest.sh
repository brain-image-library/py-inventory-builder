#!/bin/bash

#!/bin/bash
#SBATCH -p compute
#SBATCH -n 32
#SBATCH --mem=128G

# Description:
# This script is designed to aid @icaoberg in running it as a cronjob.

# Load Conda environment
if [ "$USER" = "icaoberg" ] && [ -f "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh" ]; then
	. /bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh
	conda activate base

	cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f9 | grep /bil/data/ | awk 'NR >= 1 && NR <= 1000'  | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 30 --compress --update
fi
