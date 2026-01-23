#!/bin/bash

#SBATCH -p validation
#SBATCH -n 20
#SBATCH --mem=512G

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f15 | grep /bil/data/ | awk 'NR >= 4 && NR <= 25'  | xargs -n 1 -P 5 -I {} python ./manifest-builder.py -d {} -n 4 --update --compress
