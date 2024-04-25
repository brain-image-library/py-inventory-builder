#!/bin/bash

# Set the directory path where JSON files are located
DIRECTORY=../json/

# Check if the specified directory exists
if [ -d "$DIRECTORY" ]; then
    # Search for lines containing "award_number" in all JSON files within the directory
    # Extract the third field after ':' using 'cut'
    # Remove any trailing commas using 'sed'
    # Sort the output and count unique occurrences
    grep -m 1 -R "award_number" "$DIRECTORY"/*.json | cut -d":" -f3 | sed 's|,||g' | sort | uniq -c
fi