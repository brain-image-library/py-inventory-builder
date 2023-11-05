#!/bin/bash

DIRECTORY=/Users/icaoberg/Documents/code/inventory-scripts/json/

grep -m 1 -R "award_number" $DIRECTORY/*.json | cut -d":" -f3 | sed 's|,||g' | sort | uniq -c
