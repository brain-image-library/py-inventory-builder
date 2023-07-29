#!/bin/bash

#SBATCH -p validation
#SBATCH -n 30
#SBATCH --mem=60000M

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f15 | grep /bil/data/ | awk 'NR >= 0 && NR <= 500'  | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 30 --update --compress --avoid-checksums
