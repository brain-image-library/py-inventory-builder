#!/bin/bash

#SBATCH -p validation
#SBATCH -n 20
#SBATCH --mem=128G

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f4 | grep /bil/data/ | awk 'NR >= 4500 && NR <= 5000'  | xargs -n 1 -P 2 -I {} python ./manifest-builder.py -d {} -n 10 --update --compress
