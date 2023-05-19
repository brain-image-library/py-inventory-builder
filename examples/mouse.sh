#!/bin/bash

#SBATCH -p compute
#SBATCH -n 50
#SBATCH --mem=1000Gb

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f4 | grep mouse | xargs -n 1 -I {} -P 5 python ./manifest-builder.py -d {} -n 10 --update --compress

