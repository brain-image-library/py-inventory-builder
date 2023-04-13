import argparse
import dask.dataframe as dd
import numpy as np
import pandas as pd
from pathlib import Path
import tabulate
import uuid
import requests
from pandarallel import pandarallel
from datetime import datetime


def __pprint(msg):
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


def __get_temp_file(directory):
    if directory[-1] == "/":
        directory = directory[:-1]
    file = directory.replace("/", "_")
    output_filename = f".data/{file}.tsv"

    if Path(output_filename).exists():
        return output_filename
    else:
        return None


def __get_json_file(directory):
    dataset_uuid = generate_dataset_uuid(directory)
    output_filename = f"/bil/data/inventory/{dataset_uuid}.json"

    if Path(output_filename).exists():
        return output_filename
    else:
        return None


def __get_md5_coverage(directory):
    output_filename = __get_temp_file(directory)
    if output_filename is not None and Path(output_filename).exists():
        df = pd.read_csv(output_filename, sep="\t")
        if "md5" in df.keys():
            return (len(df) - df["md5"].isnull().sum()) / len(df)
        else:
            return 0
    else:
        return 0


def __get_sha256_coverage(directory):
    output_filename = __get_temp_file(directory)
    if output_filename is not None and Path(output_filename).exists():
        df = pd.read_csv(output_filename, sep="\t")
        if "sha256" in df.keys():
            return (len(df) - df["sha256"].isnull().sum()) / len(df)
        else:
            return 0
    else:
        return 0


def __compute_score(datum):
    score = 0
    if "json_file" in datum.keys() and datum["json_file"] is not None:
        score = score + 1

    if "md5_coverage" in datum.keys():
        score = (score + datum["md5_coverage"] + datum["sha256_coverage"]) / 3.0

    return score


###############################################################################################################
__pprint(f"Processing summary metadata from brainimagelibrary,org")
url = "https://submit.brainimagelibrary.org/search/summarymetadata"
temp_file = Path(f"/tmp/summarymetadata.csv")

if temp_file.exists():
    temp_file.unlink()

ncores = 10
pandarallel.initialize(progress_bar=True, nb_workers=ncores)

response = requests.get(url)
temp_file.write_bytes(response.content)

df = pd.read_csv(temp_file, sep=",")
print("Populating dataframe with dataset UUIDs")
df["dataset_uuid"] = df["bildirectory"].parallel_apply(generate_dataset_uuid)
print("Checking if BIL directory exists")
df["exists"] = df["bildirectory"].parallel_apply(exists)

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

print("\nComputing temp file filename")
df["temp_file"] = df["bildirectory"].parallel_apply(__get_temp_file)

print("\nComputing json file filename")
df["json_file"] = df["bildirectory"].parallel_apply(__get_json_file)

# print('\nComputing MD5 coverage')
# df['md5_coverage'] = df['bildirectory'].parallel_apply(__get_md5_coverage)

# print('\nComputing SHA256 coverage')
# df['sha256_coverage'] = df['bildirectory'].parallel_apply(__get_sha256_coverage)

print("\nComputing dataset score")
for index, datum in df.iterrows():
    df.loc[index, "score"] = __compute_score(datum)

df = df.sort_values("score")
try:
    df = df[
        [
            "score",
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
            "temp_file",
            "json_file",
            "md5_coverage",
            "sha256_coverage",
        ]
    ]
except:
    df = df[
        [
            "score",
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
            "temp_file",
            "json_file",
        ]
    ]

print("Saving dataframe to disk")
df.to_csv("summary_metadata.tsv", sep="\t", index=False)

print("Exporting dataframe to Excel spreadsheet")
now = datetime.now()
df.to_excel(
    "summary_metadata.xlsx", sheet_name=str(now.strftime("%Y%m%d")), index=False
)
