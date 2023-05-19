#!/bin/bash
#SBATCH -p validation
#SBATCH -n 25
#SBATCH -N 1
#SBATCH --mem=512G

python ./update-summary.py
