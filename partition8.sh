#!/bin/bash

#SBATCH -p validation
#SBATCH -n 20
#SBATCH --mem=128G

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f15 | grep /bil/data/ | awk 'NR >= 4000 && NR <= 4500'  | xargs -n 1 -P 2 -I {} python ./manifest-builder.py -d {} -n 10 --update --compress
