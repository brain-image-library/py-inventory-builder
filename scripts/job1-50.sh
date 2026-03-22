#!/bin/bash

#SBATCH -p compute
#SBATCH -n 32
#SBATCH --mem=32G

. "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh"

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f10 | grep "/bil/data/" | awk 'NR >= 1 && NR <= 50'  | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 32  --compress --update
