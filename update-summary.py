import argparse

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pathlib import Path
import tabulate
import uuid
import requests
from pandarallel import pandarallel


def pprint(msg):
    row = len(msg)
    h = "".join(["+"] + ["-" * row] + ["+"])
    result = h + "\n" "|" + msg + "|" "\n" + h
    print(result)


def __update_dataframe(dataset, temp, key):
    for index, datum in temp.iterrows():
        dataset.loc[index, key] = temp.loc[index, key]
    return dataset


def __get_files(directory):
    files = subprocess.check_output(["lfs", "find", "-type", "f", directory])
    files = str(files)
    files = files.replace("b'/bil/data/", "/bil/data/")
    files = files.split("\\n")
    files = files[:-1]
    return files


def __get_number_of_files(directory):
    return len(files)


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

# ncores=5
# pandarallel.initialize(progress_bar=True, nb_workers=ncores)

response = requests.get(url)
temp_file.write_bytes(response.content)

df = pd.read_csv(temp_file, sep=",")
df["dataset_uuid"] = df["bildirectory"].apply(generate_dataset_uuid)
df["exists"] = df["bildirectory"].apply(exists)

df2 = df[df["exists"] == False]
df2 = df2[
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
df2.to_csv("brokensummarymetadata.tsv", sep="\t", index=False)
