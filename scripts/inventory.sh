#!/bin/bash
#SBATCH -p validation
#SBATCH -n 24
#SBATCH --mem=128Gb

. "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh"

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f10 | grep "/bil/data/" | shuf -n 500 | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 24 --compress --update
