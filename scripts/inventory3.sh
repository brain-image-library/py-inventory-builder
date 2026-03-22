#!/bin/bash
#SBATCH -p validation
#SBATCH -n 32
#SBATCH --mem=128Gb

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f9 | grep "/bil/data/" | awk 'NR >= 245 && NR <= 300'  | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 20  --compress --update
