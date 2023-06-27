#!/bin/bash
#SBATCH -p validation
#SBATCH -n 25
#SBATCH -N 1
#SBATCH --mem=512G

# this is only mean to aid @icaoberg run this as a cronjob

. /bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh
conda activate base
python ./update-summary.py
