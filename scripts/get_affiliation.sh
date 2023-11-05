#!/bin/bash

DIRECTORY=/Users/icaoberg/Documents/code/inventory-scripts/json/

grep -m 1 -R "affiliation" $DIRECTORY/*.json | cut -d":" -f3 | sed 's|,||g' | sort | uniq -c | sort -n
