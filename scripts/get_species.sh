#!/bin/bash

DIRECTORY=/Users/icaoberg/Documents/code/inventory-scripts/json/

cat $DIRECTORY/*.json | jq . `del(.manifest)` | grep -m 1 -R "species" $DIRECTORY/*.json | cut -d":" -f3 | sed 's|,||g' | sed 's|"||g' | sort | uniq -c | sort -n
