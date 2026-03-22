#!/bin/bash
#SBATCH --job-name=compress_data
#SBATCH --partition=compute
#SBATCH --output=compress_data_%j.out
#SBATCH --error=compress_data_%j.err
#SBATCH --ntasks=1
#SBATCH --time=00:10:00

# Get today's date in YYYYMMDD format
DATE=$(date +%Y%m%d)

# Output filename
ARCHIVE="data-$DATE.zip"

# Remove matching files inside .data if they exist
find .data -type f \( -name "*computing" -o -name "*done" \) -exec rm -f {} +

# Create zip archive
# -r = recursive, -q = quiet (no file list), -FS updates existing zip (freshen)
zip -r -q "$ARCHIVE" .data

# Check if zip was created successfully
if [ -f "$ARCHIVE" ]; then
    echo "Archive created: $ARCHIVE"
fi
