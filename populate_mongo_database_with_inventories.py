from sys import exit
import json
import subprocess
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from math import isnan
import zipfile

# Connect to MongoDB on BIL
client = MongoClient("mongodb://localhost:27017/")

# Send a ping to confirm a successful connection
try:
    client.admin.command("ping")
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    exit()

db = client["brainimagelibrary"]
collection = db["files"]

files = Path("JSON").glob("*.json")

inventory = []
for file in files:
    bildid = str(file).replace('JSON/','').replace('.json','')
    print(f'Processing dataset with ID {bildid}')
    command = f"jq '.manifest' {file} > {str(file).replace('JSON','/local')}"
    temp_filename = f"{str(file).replace('JSON','/local')}"
    # Use subprocess to execute the jq command

    try:
        # Run the jq command
        subprocess.run(command, shell=True, check=True)
        print(
            f"The 'manifest' field has been successfully extracted and saved to /local/."
        )
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the jq command: {e}")
        exit()

    # Load the JSON data
    try:
        with open(temp_filename, "r") as f:
            data = json.load(f)  # Parse the JSON content into a Python dictionary

        for datum in data:
            fullpath=datum["fullpath"]
            # Check if the document with the same bildid exists
            existing_document = collection.find_one({"fullpath": fullpath})
            if existing_document is None:  # If no document with the same bildid exists, insert
                print(f"File {fullpath} is not present. Populating database.")
                datum["bildid"] = bildid
                result = collection.insert_one(datum)
                print(f"Document inserted with MongoDB ID {result.inserted_id}")
            else:
                print(f"File {fullpath} is present, skipping insertion.")
    except FileNotFoundError:
        print(f"Error: The file {temp_file} was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode the JSON. Please check the file format.")
    except Exception as e:
        print(f"An error occurred: {e}")

print("Data inserted/updated successfully.")
