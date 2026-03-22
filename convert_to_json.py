#!/usr/bin/env python3
"""
sync_json_paths.py
------------------
This script synchronizes JSON files from OLD_PATH to NEW_PATH.

Behavior:
    - Recursively scans OLD_PATH for all *.json files.
    - Recreates the same relative directory structure in NEW_PATH.
    - Copies JSON files from OLD_PATH to NEW_PATH.
    - Optionally replaces internal path strings within JSON content
      (if any references still point to OLD_PATH).

Example:
    $ python sync_json_paths.py

Dependencies:
    - Python 3.7+
    - pathlib
    - json
    - shutil

Author:
    @icaoberg (modified)
"""

from pathlib import Path
import shutil
import json

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------
OLD_PATH = Path("/bil/pscstaff/icaoberg/bil-inventory/json")
NEW_PATH = Path("/bil/data/inventory/datasets/JSON")

# -------------------------------------------------------------------------
# Main logic
# -------------------------------------------------------------------------
if not OLD_PATH.exists():
    print(f"Source path '{OLD_PATH}' not found. No action taken.")
else:
    # Create NEW_PATH if needed
    NEW_PATH.mkdir(parents=True, exist_ok=True)

    json_files = list(OLD_PATH.rglob("*.json"))
    if not json_files:
        print(f"No JSON files found in '{OLD_PATH}'.")
    else:
        print(f"Found {len(json_files)} JSON file(s) to sync...")

        for src_file in json_files:
            # Determine destination file path preserving relative structure
            rel_path = src_file.relative_to(OLD_PATH)
            dest_file = NEW_PATH / rel_path

            # Ensure destination directories exist
            dest_file.parent.mkdir(parents=True, exist_ok=True)

            # Copy the file
            shutil.copy2(src_file, dest_file)

            # Optional: replace internal OLD_PATH references within JSON text
            try:
                with open(dest_file, "r+", encoding="utf-8") as f:
                    content = f.read()
                    if OLD_PATH.as_posix() in content:
                        content = content.replace(OLD_PATH.as_posix(), NEW_PATH.as_posix())
                        f.seek(0)
                        f.write(content)
                        f.truncate()
            except Exception as e:
                print(f"Warning: Could not process '{dest_file}': {e}")

        print(f"✅ Synced JSON files from '{OLD_PATH}' to '{NEW_PATH}'.")
