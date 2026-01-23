#!/bin/bash

. "/bil/users/icaoberg/miniconda3/etc/profile.d/conda.sh"
#python ./backup.py

#rclone copyto --update --verbose --transfers 30 --checkers 8 --contimeout 60s --timeout 300s --retries 3 --low-level-retries 10 --stats 1s temp/ GDrive:/BIL/reports/valid-public-datasets-statistics
