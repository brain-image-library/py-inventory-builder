import pymongo
import argparse
import pdb
import brainimagelibrary as brainzzz
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
import sys
import json
import traceback
from tqdm import tqdm
import numpy as np
import pandas as pd
import requests
import tabulate
from pandarallel import pandarallel

def __pprint(msg):
    """
    Pretty prints a message with a surrounding border of '+' and '-' characters.

    Args:
        msg (str): The message to be printed.

    Returns:
        None
    """
    row = len(msg)
    h = "".join(["+"] + ["-" * row] + ["+"])  # Create the border line
    result = h + "\n" "|" + msg + "|" "\n" + h  # Construct the formatted message
    print(result)  # Print the formatted message


def __get_data(json_file):
    """
    Reads and loads JSON data from a specified file into a dictionary.

    Args:
        json_file (str): Path to the JSON file.

    Returns:
        dict: A dictionary containing the loaded JSON data, or an empty dictionary if the file cannot be read.
    """
    try:
        with open(json_file, "r") as file:
            data = json.load(file)
        return data
    except:
        return {}


def __get_mime_types(filename):
    """
    Extracts MIME type frequencies from a JSON file containing 'manifest' data.

    Parameters:
    filename (str): Path to the JSON file containing 'manifest' data.

    Returns:
    dict: A dictionary where keys are MIME types and values are their respective frequencies.
          The frequencies represent the number of occurrences of each MIME type in the file.
    """

    try:
        # Load JSON data from file
        with open(filename, "r") as f:
            json_data = json.load(f)

        # Extract the 'manifest' field into a DataFrame
        data = json_data["manifest"]
        df = pd.DataFrame(data)

        # Count occurrences of each MIME type and convert to dictionary
        mime_type_counts = df["mime-type"].value_counts().to_dict()

        return mime_type_counts
    except:
        traceback.print_exc()
        return {}


def __get_file_types(filename):
    """
    Extracts file type frequencies from a JSON file containing 'manifest' data.

    Parameters:
    filename (str): Path to the JSON file containing 'manifest' data.

    Returns:
    dict: A dictionary where keys are file types and values are their respective frequencies.
          The frequencies represent the number of occurrences of each file type in the file.
    """

    try:
        # Load JSON data from file
        with open(filename, "r") as f:
            json_data = json.load(f)

        # Extract the 'manifest' field into a DataFrame
        data = json_data["manifest"]
        df = pd.DataFrame(data)

        # Count occurrences of each file type and convert to dictionary
        file_type_counts = df["filetype"].value_counts().to_dict()

        return file_type_counts
    except:
        return None


def __get_frequencies(filename):
    """
    Extracts frequencies of file extensions from a JSON file containing 'manifest' data.

    Parameters:
    filename (str): Path to the JSON file containing 'manifest' data.

    Returns:
    dict: A dictionary where keys are file extensions and values are their respective frequencies.
          The frequencies represent the number of occurrences of each file extension in the file.
    """

    try:
        # Load JSON data from file
        with open(filename, "r") as f:
            json_data = json.load(f)

        # Extract the 'manifest' field into a DataFrame
        data = json_data["manifest"]
        df = pd.DataFrame(data)

        # Count occurrences of each file extension and convert to dictionary
        extension_counts = df["extension"].value_counts().to_dict()

        return extension_counts
    except:
        return {}


def __get_dataset_size(data):
    """
    Retrieves the 'size' attribute from a JSON data dictionary.

    Args:
        data (dict): The JSON data dictionary.

    Returns:
        int or None: Size of the dataset, or None if 'size' attribute is missing or invalid.
    """
    try:
        return data["size"]
    except:
        return None


def __get_pretty_dataset_size(data):
    """
    Retrieves the 'pretty_size' attribute from a JSON data dictionary.

    Args:
        data (dict): The JSON data dictionary.

    Returns:
        str or None: Pretty size of the dataset, or None if 'pretty_size' attribute is missing or invalid.
    """
    try:
        return data["pretty_size"]
    except:
        return None


def __get_creation_date(data):
    """
    Retrieves the 'creation_date' attribute from a JSON data dictionary.

    Args:
        data (dict): The JSON data dictionary.

    Returns:
        str or None: Creation date of the dataset, or None if 'creation_date' attribute is missing or invalid.
    """
    try:
        return data["creation_date"]
    except:
        return None


def __update_dataframe(dataset, temp, key):
    """
    Update a DataFrame ('dataset') with values from another DataFrame ('temp') based on a specified 'key'.

    Args:
        dataset (pandas.DataFrame): The target DataFrame to be updated.
        temp (pandas.DataFrame): The DataFrame containing updated values.
        key (str): The column key used for updating.

    Returns:
        pandas.DataFrame: The updated DataFrame ('dataset').
    """
    for index, datum in temp.iterrows():
        dataset.loc[index, key] = temp.loc[index, key]
    return dataset


def __get_files(directory):
    """
    Retrieves a list of files within a directory using the 'lfs find' command.

    Args:
        directory (str): The directory path.

    Returns:
        list: A list of file paths within the directory.
    """
    files = subprocess.check_output(["lfs", "find", "-type", "f", directory])
    files = str(files)
    files = files.replace("b'/bil/data/", "/bil/data/")
    files = files.split("\\n")
    files = files[:-1]
    return files


def __get_number_of_files(directory):
    """
    Retrieves the number of files within a directory.

    Args:
        directory (str): The directory path.

    Returns:
        int: The number of files within the directory.
    """
    return len(__get_files(directory))


def generate_dataset_uuid(directory):
    """
    Generates a UUID for a given directory path.

    Args:
        directory (str): The directory path.

    Returns:
        str: The generated UUID.
    """
    if directory[-1] == "/":
        directory = directory[:-1]

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, directory))


def exists(directory):
    """
    Checks if a directory exists.

    Args:
        directory (str): The directory path.

    Returns:
        bool: True if the directory exists, False otherwise.
    """
    try:
        return Path(directory).exists()
    except:
        return False


def __get_temp_file(directory):
    """
    Generates a temporary file path based on a directory path.

    Args:
        directory (str): The directory path.

    Returns:
        str or None: The temporary file path, or None if the file does not exist.
    """
    if directory[-1] == "/":
        directory = directory[:-1]
    file = directory.replace("/", "_")
    output_filename = f".data/{file}.tsv"

    if Path(output_filename).exists():
        return output_filename
    else:
        return None


def __get_json_file(directory):
    """
    Generates a JSON file path based on a directory path using a generated UUID.

    Args:
        directory (str): The directory path.

    Returns:
        str or None: The JSON file path, or None if the file does not exist.
    """
    dataset_uuid = generate_dataset_uuid(directory)
    output_filename = f"/bil/data/inventory/{dataset_uuid}.json"

    if Path(output_filename).exists():
        return output_filename
    else:
        return None


def __get_md5_coverage(directory):
    """
    Calculates coverage based on the 'md5' hash from a DataFrame.

    Args:
        directory (str): The directory path.

    Returns:
        float or None: Coverage percentage of 'md5' hash, or None if unable to process.
    """
    try:
        output_filename = __get_temp_file(directory)
        if output_filename is not None and Path(output_filename).exists():
            df = pd.read_csv(output_filename, sep="\t", low_memory=False)
            if "md5" in df.columns:
                return (len(df) - df["md5"].isnull().sum()) / len(df)
            else:
                return 0
        else:
            return 0
    except:
        print(f"Unable to process file {output_filename}")
        return None


def __get_sha256_coverage(directory):
    """
    Calculates coverage based on the 'sha256' hash from a DataFrame.

    Args:
        directory (str): The directory path.

    Returns:
        float or None: Coverage percentage of 'sha256' hash, or None if unable to process.
    """
    try:
        output_filename = __get_temp_file(directory)
        if output_filename is not None and Path(output_filename).exists():
            df = pd.read_csv(output_filename, sep="\t", low_memory=False)
            if "sha256" in df.columns:
                return (len(df) - df["sha256"].isnull().sum()) / len(df)
            else:
                return 0
        else:
            return 0
    except:
        print(f"Unable to process file {output_filename}")
        return None


def __get_xxh64_coverage(directory):
    """
    Calculates coverage based on the 'xxh64' hash from a DataFrame.

    Args:
        directory (str): The directory path.

    Returns:
        float or None: Coverage percentage of 'xxh64' hash, or None if unable to process.
    """
    try:
        output_filename = __get_temp_file(directory)
        if output_filename is not None and Path(output_filename).exists():
            df = pd.read_csv(output_filename, sep="\t", low_memory=False)
            if "xxh64" in df.columns:
                return (len(df) - df["xxh64"].isnull().sum()) / len(df)
            else:
                return 0
        else:
            return 0
    except:
        print(f"Unable to process file {output_filename}")
        return None


def __compute_score(datum):
    """
    Computes a score based on attributes in a 'datum' dictionary.

    Args:
        datum (dict): The data dictionary containing attributes.

    Returns:
        float: Computed score based on specified attributes.
    """
    score = 0

    if "json_file" in datum.keys() and datum["json_file"] is not None:
        score += 1

    if "md5_coverage" in datum.keys():
        coverage_sum = (
            datum["md5_coverage"] + datum["sha256_coverage"] + datum["xxh64_coverage"]
        )
        score = coverage_sum / 3.0

    return score


###############################################################################################################
# Check if backup already
###############################################################################################################
parser = argparse.ArgumentParser()
parser.add_argument(
    "-n", "--number-of-cores", dest="ncores", help="Number of cores", default=12
)

parser.add_argument(
    "--sample", action=argparse.BooleanOptionalAction, dest="sample", default=False
)

args = parser.parse_args()
sample = args.sample
ncores = args.ncores

if Path("/bil/data/inventory").exists():
    now = datetime.now()
    report_output_directory = "/bil/data/inventory/daily/reports"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
    )

__pprint(f"Processing summary metadata from brainimagelibrary.org")
pandarallel.initialize(progress_bar=True, nb_workers=ncores)

report = brainzzz.reports.__create_daily_report()
df = pd.DataFrame(report)

# sample some rows for testing
if sample:
    df = df.sample(frac=0.1, replace=True)

# clean dataframe
if "title" in df.keys():
    df = df.drop("title", axis=1)

if "abstract" in df.keys():
    df = df.drop("abstract", axis=1)

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

# print("\nGetting number of files")
# df["number_of_files"] = df["bildirectory"].parallel_apply(__get_number_of_files)
df["number_of_files"] = None

# df["file_types"] = str(df["json_file"].parallel_apply(__get_file_types))
# df["frequencies"] = str(df["json_file"].parallel_apply(__get_frequencies))
# df["mimetypes"] = str(df["json_file"].parallel_apply(__get_mime_types))

df["temp_file"] = str(df["bildirectory"].parallel_apply(__get_temp_file))

df["file_types"] = None
df["frequencies"] = None
df["mime_types"] = None

# print("\nComputing MD5 coverage")
# df["md5_coverage"] = df["bildirectory"].parallel_apply(__get_md5_coverage)
df["md5_coverage"] = None

# print("\nComputing SHA256 coverage")
# df["sha256_coverage"] = df["bildirectory"].parallel_apply(__get_sha256_coverage)
df["sha256_coverage"] = None

# print("\nComputing xxh64 coverage")
# df["xxh64_coverage"] = df["bildirectory"].parallel_apply(__get_xxh64_coverage)
df["xxh64_coverage"] = None

# print("\nComputing dataset score")
# for index, datum in df.iterrows():
#    df.loc[index, "score"] = __compute_score(datum)
# df = df.sort_values("score")
df["score"] = None

print("Saving dataframe to disk")
df.to_csv("summary_metadata.tsv", sep="\t", index=False)

# saves to public directory
if not sample and Path("/bil/data/inventory").exists():
    print("Creating JSON")
    now = datetime.now()
    report_output_directory = "/bil/data/inventory/daily/reports"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.json'
    )
    df.to_json(report_output_filename, orient="records", indent=4)

    symlink_file = f"{report_output_directory}/today.json"
    if Path(symlink_file).exists():
        Path(symlink_file).unlink()

    command = f"rm -fv {symlink_file} & ln -s {report_output_filename} {symlink_file}"
    print(command)
    output = subprocess.check_output(command, shell=True)

    print("Backing up data to /bil/data/inventory")
    print("Creating TSV file")
    df.drop(columns=["file_types", "frequencies"], inplace=True)
    now = datetime.now()

    report_output_directory = "/bil/data/inventory/daily/reports"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
    )

    command = f"rm -fv {symlink_file} & ln -s {report_output_filename} {symlink_file}"
    print(command)
    output = subprocess.check_output(command, shell=True)

    symlink_file = f"{report_output_directory}/today.tsv"
    if Path(symlink_file).exists():
        Path(symlink_file).unlink()

    df.to_csv(report_output_filename, sep="\t", index=False)

print("Exporting dataframe to Excel spreadsheet")
now = datetime.now()
df.to_excel(
    "summary_metadata.xlsx", sheet_name=str(now.strftime("%Y%m%d")), index=False
)
