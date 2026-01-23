#!/bin/bash
#SBATCH -p compute
#SBATCH -n 25
#SBATCH --mem=50000M

# Description:
# This script is designed to aid @icaoberg in running it as a cronjob.

# Load Conda environment
if [ "$USER" = "icaoberg" ] && [ -f "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh" ]; then
	. /bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh
	conda activate base

    python ./update-missing-fields.py
else
	echo "Only user icaoberg can run this script"
fi
