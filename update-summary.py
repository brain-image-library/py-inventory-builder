import argparse

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pathlib import Path
import tabulate
import uuid
import requests


def pprint(msg):
    row = len(msg)
    h = "".join(["+"] + ["-" * row] + ["+"])
    result = h + "\n" "|" + msg + "|" "\n" + h
    print(result)


def __update_dataframe(dataset, temp, key):
    for index, datum in temp.iterrows():
        dataset.loc[index, key] = temp.loc[index, key]
    return dataset


def generate_dataset_uuid(directory):
    if directory[-1] == "/":
        directory = directory[:-1]

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, directory))


def exists(directory):
    return Path(directory).exists()


###############################################################################################################
url = "https://submit.brainimagelibrary.org/search/summarymetadata"
temp_file = Path(f"/tmp/summarymetadata.csv")

if temp_file.exists():
    temp_file.unlink()

response = requests.get(url)
temp_file.write_bytes(response.content)

df = pd.read_csv(temp_file, sep=",")
df["dataset_uuid"] = df["bildirectory"].apply(generate_dataset_uuid)
df["exists"] = df["bildirectory"].apply(exists)
df = df[df["exists"] == True]

df = df[
    [
        "metadata_version",
        "dataset_uuid",
        "bildirectory",
        "exists",
        "project",
        "is_biccn",
        "bil_uuid",
        "contributorname",
        "affiliation",
        "award_number",
        "species",
        "ncbitaxonomy",
        "samplelocalid",
        "genotype",
        "generalmodality",
        "technique",
        "locations",
        "URL",
    ]
]
df.to_csv("summarymetadata.tsv", sep="\t", index=False)
