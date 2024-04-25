#!/bin/bash

# Set the directory path
DIRECTORY=../json/

# Check if the directory exists
if [ -d "$DIRECTORY" ]; then
    # Search for lines containing "affiliation" in all JSON files within the directory
    # Extract the third field after ':' using 'cut'
    # Remove any trailing commas using 'sed'
    # Sort the output and count unique occurrences
    grep -m 1 -R "affiliation" "$DIRECTORY"/*.json | cut -d":" -f3 | sed 's|,||g' | sort | uniq -c | sort -n
fi