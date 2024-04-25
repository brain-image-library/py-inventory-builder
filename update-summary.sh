#!/bin/bash
#SBATCH -p compute
#SBATCH -n 25
#SBATCH --mem=50000M

# this is only mean to aid @icaoberg run this as a cronjob

. /bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh
conda activate base
python ./update-summary.py
python ./create_list_file.py

sed -i 's|"Allen Institute for Brain Science "|"Allen Institute for Brain Science"|g' today.json
sed -i 's|"award_number":null|"award_number":"Unavailable"|g' today.json
sed -i 's|"Mouse"|"mouse"|g' today.json
sed -i 's|"Human"|"human"|g' today.json
sed -i 's|"Marmoset"|"marmoset"|g' today.json
sed -i 's|"other"|"Other"|g' today.json

if [ -f /bil/data/inventory/daily/reports/today.json ]; then
	rm -f /bil/data/inventory/daily/reports/today.json
	cp today.json /bil/data/inventory/daily/reports/
fi
