#!/bin/bash

#SBATCH -p RM-shared
#SBATCH --time "12:00:00"
#SBATCH --mem=20000M
#SBATCH -n 10

if [ ! -f json-$(date +%Y%m%d).tgz ]; then
	tar -cvf json-$(date +%Y%m%d).tgz /bil/pscstaff/icaoberg/inventories/json
fi

if [ ! -f -data-$(date +%Y%m%d).tgz ]; then
	tar -cvf data-$(date +%Y%m%d).tgz /bil/pscstaff/icaoberg/inventories/data
fi
