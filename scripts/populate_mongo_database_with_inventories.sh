#!/bin/bash

#SBATCH -p compute
#SBATCH -n 8
#SBATCH --mem=16G

. "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh"

cd /bil/pscstaff/icaoberg/databases/mongo
bash ./start.sh

cd /bil/pscstaff/icaoberg/bil-inventory/
python ./populate_mongo_database_with_inventories.py

cd /bil/pscstaff/icaoberg/databases/mongo
bash ./stop.sh
