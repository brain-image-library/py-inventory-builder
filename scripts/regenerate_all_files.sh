#!/bin/bash

#SBATCH -p compute
#SBATCH -n 32
#SBATCH --mem=32G

. "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh"

cat summary_metadata.tsv | grep -v bildirectory | cut -d$'\t' -f9 | grep "/bil/data/" | xargs -n 1 -P 1 -I {} python ./manifest-builder.py -d {} -n 32  --compress --update --remove-checkpoints --avoid-checksums
