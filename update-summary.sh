#!/bin/bash
#SBATCH -p validation
#SBATCH -n 25
#SBATCH --mem=50000M

# this is only mean to aid @icaoberg run this as a cronjob

. /bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh
conda activate base
python ./update-summary.py
