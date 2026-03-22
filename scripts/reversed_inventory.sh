#!/bin/bash
#SBATCH -p compute
#SBATCH -n 16
#SBATCH --mem=128Gb

cat temp.tsv | grep -v bildirectory | cut -d$'\t' -f9 | grep "/bil/data/" | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 16  --compress --update
