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

try:
    # Check if 'datasets' collection exists
    if "datasets" in db.list_collection_names():
        print("Collection 'datasets' exists. Dropping it...")
        db["datasets"].drop()
        print("Collection 'datasets' dropped.")
    else:
        print("Collection 'datasets' does not exist. Proceeding with rename.")

    # Check if 'dataset' exists before renaming
    if "dataset" in db.list_collection_names():
        print("Renaming 'dataset' to 'datasets'...")
        db["dataset"].rename("datasets")
        print("Collection 'dataset' successfully renamed to 'datasets'.")
    else:
        print("Error: Collection 'dataset' does not exist. Nothing to rename.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection
    client.close()
    print("MongoDB connection closed.")
