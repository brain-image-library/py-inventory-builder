import json
import subprocess
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from math import isnan
import zipfile

# Connect to MongoDB
client = MongoClient("mongodb://vm013.bil.psc.edu:27017/")

# Send a ping to confirm a successful connection
try:
    client.admin.command("ping")
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client["brainimagelibrary"]
collection = db["datasets"]

files = Path("JSON").glob("*.json")

for file in files:
    command = f"jq 'del(.manifest)' {file} > {str(file).replace('JSON','/local')}"
    print(command)
    temp_filename = f"{str(file).replace('JSON','/local')}"
    print(temp_filename)
    # Use subprocess to execute the jq command

    try:
        # Run the jq command
        subprocess.run(command, shell=True, check=True)
        print(
            f"The 'manifest' field has been successfully removed and saved to /local/{file}."
        )
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the jq command: {e}")

    # Load the JSON data
    try:
        with open(temp_filename, "r") as f:
            data = json.load(f)  # Parse the JSON content into a Python dictionary

        # Get the value of 'bildid'
        bildid = data.get(
            "bildid"
        )  # Use .get() to avoid KeyError if 'bildid' does not exist

        if bildid is not None:
            print(f"The value of 'bildid' is: {bildid}")
        else:
            print("'bildid' key not found in the JSON file.")
    except FileNotFoundError:
        print(f"Error: The file {temp_file} was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode the JSON. Please check the file format.")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Check if the document with the same bildid exists
    existing_document = collection.find_one({"bildid": bildid})

    if existing_document is None:  # If no document with the same bildid exists, insert
        print(f"Document with bildid {bildid} does not exist. Populating database.")
        result = collection.insert_one(data)  # Insert a single document
        print(f"Document inserted with MongoDB ID {result.inserted_id}")
    else:
        print(f"Document with bildid {bildid} already exists, skipping insertion.")
print("Data inserted/updated successfully.")
