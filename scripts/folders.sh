#!/bin/bash

#SBATCH -p validation
#SBATCH -n 32
#SBATCH --mem=128G

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f9 | grep "/bil/data/" | awk 'NR >= 1 && NR <= 1000'  | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 32  --compress --update --remove-checkpoints
