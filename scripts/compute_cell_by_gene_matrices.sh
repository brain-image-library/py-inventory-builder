#!/bin/bash

#SBATCH -p validation

cd .data && grep -R "cell_by_" *tsv > ../cell_by_gene_list.tsv
