import subprocess
import uuid
from datetime import datetime
from pathlib import Path
import sys
import json
from tqdm import tqdm
import numpy as np
import pandas as pd
import requests
import tabulate
from pandarallel import pandarallel


def __pprint(msg):
    row = len(msg)
    h = "".join(["+"] + ["-" * row] + ["+"])
    result = h + "\n" "|" + msg + "|" "\n" + h
    print(result)


def __get_data(json_file):
    try:
        # Open the JSON file
        with open(json_file, "r") as file:
            # Load the JSON data into a dictionary
            data = json.load(file)

        return data
    except:
        return {}


def __get_file_types(data):
    try:
        return data["file_types"]
    except:
        return None


def __get_frequencies(data):
    try:
        return data["frequencies"]
    except:
        return None


def __get_dataset_size(data):
    try:
        return data["size"]
    except:
        return None


def __get_pretty_dataset_size(data):
    try:
        return data["pretty_size"]
    except:
        return None


def __get_creation_date(data):
    try:
        return data["creation_date"]
    except:
        return None


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
    return len(__get_files(directory))


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
    try:
        output_filename = __get_temp_file(directory)
        if output_filename is not None and Path(output_filename).exists():
            df = pd.read_csv(output_filename, sep="\t", low_memory=False)
            if "md5" in df.keys():
                return (len(df) - df["md5"].isnull().sum()) / len(df)
            else:
                return 0
        else:
            return 0
    except:
        print(f"Unable to process file {output_filename}")
        return None


def __get_sha256_coverage(directory):
    try:
        output_filename = __get_temp_file(directory)
        if output_filename is not None and Path(output_filename).exists():
            df = pd.read_csv(output_filename, sep="\t", low_memory=False)
            if "sha256" in df.keys():
                return (len(df) - df["sha256"].isnull().sum()) / len(df)
            else:
                return 0
        else:
            return 0
    except:
        print(f"Unable to process file {output_filename}")
        return None


def __compute_score(datum):
    score = 0
    if "json_file" in datum.keys() and datum["json_file"] is not None:
        score = score + 1

    if "md5_coverage" in datum.keys():
        score = (score + datum["md5_coverage"] + datum["sha256_coverage"]) / 3.0

    return score


###############################################################################################################
# Check if backup already
if Path("/bil/data/inventory").exists():
    now = datetime.now()
    report_output_directory = "/bil/data/inventory/daily/reports"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
    )
    if Path(report_output_filename).exists():
        print(
            f"Backup file {report_output_filename} already exists. Skipping computation."
        )
        sys.exit()

__pprint(f"Processing summary metadata from brainimagelibrary.org")
url = "https://submit.brainimagelibrary.org/search/summarymetadata"
temp_file = Path(f"/tmp/summarymetadata.csv")

if temp_file.exists():
    temp_file.unlink()

ncores = 25
pandarallel.initialize(progress_bar=True, nb_workers=ncores)

response = requests.get(url)
temp_file.write_bytes(response.content)

df = pd.read_csv(temp_file, sep=",", low_memory=False)
if df.keys()[0] == "<html>":
    print(f"File is empty. More than likely the submit VM is down.")
    print(f"Attempting to load backup file, if it exists.")
    if Path("/bil/data/inventory").exists():
        now = datetime.now()
        report_output_directory = "/bil/data/inventory/daily"
        report_output_filename = (
            f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.csv'
        )
        if Path(report_output_filename).exists():
            temp_file = report_output_filename
            df = pd.read_csv(temp_file, sep=",", low_memory=False)
        else:
            print(f"Unable to find file {report_output_filename}. Exiting program.")
            sys.exit()

##clean dataframe
if "title" in df.keys():
    df = df.drop("title", axis=1)

if "abstract" in df.keys():
    df = df.drop("abstract", axis=1)

print("\nPopulating dataframe with dataset UUIDs")
df["dataset_uuid"] = df["bildirectory"].parallel_apply(generate_dataset_uuid)
print("\nChecking if BIL directory exists")
df["exists"] = df["bildirectory"].parallel_apply(exists)
print("\nComputing json file filename")
df["json_file"] = df["bildirectory"].parallel_apply(__get_json_file)

df = df[df["exists"] == True]

print("\nGet data from JSON file")
for index, row in tqdm(df.iterrows()):
    json_file = df.at[index, "json_file"]
    data = __get_data(json_file)
    df.loc[index, "creation_date"] = __get_creation_date(data)
    df.loc[index, "size"] = __get_dataset_size(data)
    df.loc[index, "pretty_size"] = __get_pretty_dataset_size(data)

print("\nGetting number of files")
df["number_of_files"] = df["bildirectory"].parallel_apply(__get_number_of_files)

# print("\nGetting file types")
# df["file_types"] = df["json_file"].parallel_apply(__get_file_types)

# print("\nGetting frequencies")
# df["frequencies"] = df["json_file"].parallel_apply(__get_frequencies)

print("\nComputing temp file filename")
df["temp_file"] = df["bildirectory"].parallel_apply(__get_temp_file)

print("\nComputing MD5 coverage")
df["md5_coverage"] = df["bildirectory"].parallel_apply(__get_md5_coverage)

print("\nComputing SHA256 coverage")
df["sha256_coverage"] = df["bildirectory"].parallel_apply(__get_sha256_coverage)

print("\nComputing dataset score")
for index, datum in df.iterrows():
    df.loc[index, "score"] = __compute_score(datum)

df = df.sort_values("score")

print("Saving dataframe to disk")
df.to_csv("summary_metadata.tsv", sep="\t", index=False)

# saves to public directory
if Path("/bil/data/inventory").exists():
    print("Backing up data to /bil/data/inventory")
    now = datetime.now()
    report_output_directory = "/bil/data/inventory/daily/reports"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
    )
    df.to_csv(report_output_filename, sep="\t", index=False)

    symlink_file = f"{report_output_directory}/today.tsv"
    if Path(symlink_file).exists():
        Path(symlink_file).unlink()

    command = f"ln -s {report_output_filename} {symlink_file}"
    print(command)
    output = subprocess.check_output(command, shell=True)

    now = datetime.now()
    report_output_directory = "/bil/data/inventory/daily/reports"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.json'
    )
    df.to_json(report_output_filename, orient="records", indent=4)

    symlink_file = f"{report_output_directory}/today.json"
    if Path(symlink_file).exists():
        Path(symlink_file).unlink()

    command = f"ln -s {report_output_filename} {symlink_file}"
    print(command)
    output = subprocess.check_output(command, shell=True)

print("Exporting dataframe to Excel spreadsheet")
now = datetime.now()
df.to_excel(
    "summary_metadata.xlsx", sheet_name=str(now.strftime("%Y%m%d")), index=False
)
