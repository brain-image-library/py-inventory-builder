#!/bin/bash

# Set the directory path where JSON files are located
DIRECTORY=../json/

# Check if the specified directory exists
if [ -d "$DIRECTORY" ]; then
    # Search for lines containing "contributor_name" in all JSON files within the directory
    # Extract the third field after ':' using 'cut'
    # Remove any trailing commas using 'sed'
    # Sort the output, count unique occurrences, and then sort by count in ascending order
    grep -m 1 -R "contributor_name" "$DIRECTORY"/*.json | cut -d":" -f3 | sed 's|,||g' | sort | uniq -c | sort -n
fi