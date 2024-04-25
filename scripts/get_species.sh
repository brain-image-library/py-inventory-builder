#!/bin/bash

# Set the directory path where JSON files are located
DIRECTORY=../json/

# Check if the specified directory exists
if [ -d "$DIRECTORY" ]; then
    # Concatenate all JSON files and filter with jq to remove 'manifest' key
    # Search for lines containing "species" in all JSON files within the directory
    # Extract the third field after ':' using 'cut'
    # Remove any commas (`,`) and double quotes (`"`) from the extracted values using 'sed'
    # Sort the output, count unique occurrences, and then sort by count in ascending order
    cat "$DIRECTORY"/*.json | jq 'del(.manifest)' | grep -m 1 -R "species" "$DIRECTORY"/*.json | cut -d":" -f3 | sed 's|,||g' | sed 's|"||g' | sort | uniq -c | sort -n
fi