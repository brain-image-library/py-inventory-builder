import traceback
import math
from sys import exit
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from math import isnan
import zipfile
from typing import Any

filename = 'summary_metadata.tsv'
# Connect to MongoDB
client = MongoClient("mongodb://vm013.bil.psc.edu:27017/")

def get_json_filename(bildid: str) -> str:
    return f'JSON/{bildid}.json'

def get_document(bildid: str) -> Any:
    try:
        db = client['brainimagelibrary']
        collection = db['datasets']

        document = collection.find_one({'bildid': bildid})
        return document
    except:
        return None

ttt get_value_by_key(bildid: str, key: str, client=client) -> Any:
    try:
        db = client['brainimagelibrary']
        collection = db['datasets']

        # Query the document by bildid
        document = collection.find_one({'bildid': bildid})

        # Check if document exists and key exists in document
        if document and key in document:
            return document[key]
        else:
            return None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if Path(filename).exists():
    df = pd.read_csv(filename, sep='\t', low_memory=False)
    df = df.sort_values(by="bildirectory", key=lambda x: x.str.lower())

# Insert or update documents in MongoDB
for index, row in df.iterrows():
    bildid = row['bildid']
    print(bildid)
    directory = row['bildirectory']

    try:
        size = int(get_value_by_key(bildid, 'size')['bytes'])
        print(f'Size in bytes: {size}')
        df.at[index, 'size'] = size
    except:
        traceback.print_exc()
        df.at[index, 'size'] = None

    try:
        df.at[index, 'pretty_size'] = str(get_value_by_key(bildid, 'size')['pretty'])
    except:
        df.at[index, 'pretty_size'] = None

    try:
        df.at[index, 'md5_coverage'] = str(get_value_by_key(bildid, 'coverage')['md5'])
    except:
        df.at[index, 'md5_coverage'] = None

    try:
        df.at[index, 'sha256_coverage'] = str(get_value_by_key(bildid, 'coverage')['sha256'])
    except:
        df.at[index, 'sha256_coverage'] = None

    try:
        df.at[index, 'xxh64_coverage'] = str(get_value_by_key(bildid, 'coverage')['xxh64'])
    except:
        df.at[index, 'xxh64_coverage'] = None

    try:
        df.at[index, 'b2sum_coverage'] = str(get_value_by_key(bildid, 'coverage')['b2sum'])
    except:
        df.at[index, 'b2sum_coverage'] = None

    df.at[index, 'json_file'] = get_json_filename(bildid)
    df.at[index, 'number_of_files'] = get_value_by_key(bildid, 'number_of_files')
    df.at[index, 'mime_types'] = str(get_value_by_key(bildid, 'mime-types'))
    df.at[index, 'frequencies'] = str(get_value_by_key(bildid, 'frequencies'))
    df.at[index, 'file_types'] = str(get_value_by_key(bildid, 'file_types'))
    try:
        df.at[index, 'score'] = sum(value for value in get_value_by_key(bildid, 'coverage').values() if value is not None and not (isinstance(value, float) and math.isnan(value)))/4.0
    except:
        df.at[index, 'score'] = 0.0

df.to_csv(filename, sep='\t', index=False)
client.close()
