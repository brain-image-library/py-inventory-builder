#!/bin/bash

#SBATCH -p validation
#SBATCH -n 50
#SBATCH --mem=512G

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f4 | grep /bil/data/ | xargs -n 1 -P 5 -I {} python ./manifest-builder.py -d {} -n 10 --update --compress --avoid-checksums
