#!/bin/bash
#SBATCH -p compute
#SBATCH -n 25
#SBATCH --mem=50000M

# Description:
# This script is designed to aid @icaoberg in running it as a cronjob.

# Load Conda environment
if [ "$USER" = "icaoberg" ] && [ -f "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh" ]; then
        . /bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh
        conda activate base

	# Execute Python scripts
        python ./generate_daily_report.py
        python ./update-summary.py
        python ./update-statistics.py
        python ./compute_score.py
        python ./convert_to_json.py
        #python ./update-missing-fields.py

        # Modify JSON file ('today.json') using 'sed' commands
        sed -i 's|"Allen Institute for Brain Science "|"Allen Institute for Brain Science"|g' today.json
        sed -i 's|"award_number":null|"award_number":"Unavailable"|g' today.json
        sed -i 's|"Mouse"|"mouse"|g' today.json
        sed -i 's|"Human"|"human"|g' today.json
        sed -i 's|"Marmoset"|"marmoset"|g' today.json
        sed -i 's|"other"|"Other"|g' today.json

        # Move processed JSON file to a specific directory if it exists
        if [ -f /bil/data/inventory/daily/reports/today.json ]; then
                rm -f /bil/data/inventory/daily/reports/today.json
                cp today.json /bil/data/inventory/daily/reports/
        fi
else
        echo "Only user icaoberg can run this script"
fi
