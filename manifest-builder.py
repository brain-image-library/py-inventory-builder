import argparse
import datetime
import hashlib
import json
import os
import os.path
import shutil
import time
import uuid
import warnings
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import tabulate
from dask import delayed
from joblib import Parallel, delayed
from pandarallel import pandarallel
from PIL import Image
from tqdm import tqdm


def pprint(msg):
    row = len(msg)
    h = "".join(["+"] + ["-" * row] + ["+"])
    result = h + "\n" "|" + msg + "|" "\n" + h
    print(result)


def __update_dataframe(dataset, temp, key):
    for index, datum in temp.iterrows():
        dataset.loc[index, key] = temp.loc[index, key]
    return dataset


###############################################################################################################
# Load file with files on directory
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", dest="directory", help="Directory")
parser.add_argument("-n", "--number-of-cores", dest="ncores", help="Number of cores")
args = parser.parse_args()
directory = args.directory

ncores = int(args.ncores)
pandarallel.initialize(progress_bar=True, nb_workers=ncores)

if directory[-1] == "/":
    directory = directory[:-1]

if not Path(".data").exists():
    Path(".data").mkdir()

if not Path("json").exists():
    Path("json").mkdir()

file = directory.replace("/", "_")
output_filename = ".data/" + file + ".tsv"

temp_directory = "/scratch/icaoberg/"
if not Path(temp_directory).exists():
    temp_directory = "/tmp/"

if Path(temp_directory + output_filename).exists():
    shutil.copyfile(temp_directory + output_filename, output_filename)

if Path(output_filename).exists():
    print(f"Found temporary file {output_filename}. Loading local file.")
    df = pd.read_csv(output_filename, sep="\t", low_memory=False)
else:
    print(f"Finding all files in {directory}.")
    files = [
        x
        for x in Path(directory).glob("**/*")
        if (x.is_file() or x.is_symlink()) and not x.is_dir()
    ]
    df = pd.DataFrame()
    df["fullpath"] = files

###############################################################################################################
pprint("Get file extensions")


def get_file_extension(filename):
    if Path(filename).is_file() or Path(filename).is_symlink():
        extension = Path(filename).suffix
        if extension == ".tiff" or extension == ".tif":
            if str(filename).find("ome.tif") > 0:
                extension = ".ome.tif"

    return extension


if "extension" not in df.keys():
    print("\nComputing file extension")
    df["extension"] = df["fullpath"].parallel_apply(get_file_extension)
    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Get filename")


def get_filename(filename):
    return Path(filename).stem + Path(filename).suffix


if "filename" not in df.keys():
    df["filename"] = df["fullpath"].parallel_apply(get_filename)
    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Get relative path")


def get_relative_path(full_path):
    answer = str(full_path).replace(f"{directory}/", "")
    return answer


if "relativepath" not in df.keys():
    print("\nComputing relative paths")
    df["relativepath"] = df["fullpath"].parallel_apply(get_relative_path)
    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Get file type")


def get_filetype(extension):
    images = {
        ".tiff",
        ".png",
        ".tif",
        ".ome.tif",
        ".jpeg",
        ".ims",
        ".gif",
        ".ome.tiff",
        "jpg",
        ".jp2",
    }

    if extension in images:
        return "images"

    if extension == ".swc":
        return "tracing"

    return "other"


if "filetype" not in df.keys():
    print("\nComputing file type")
    df["filetype"] = df["extension"].parallel_apply(get_filetype)
    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Get file creation date")


def get_file_creation_date(filename):
    t = os.path.getmtime(str(filename))
    return str(datetime.datetime.fromtimestamp(t))


if "modification_time" not in df.keys():
    print("\nComputing modification time")
    df["modification_time"] = df["fullpath"].parallel_apply(get_file_creation_date)
    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Get file size")


def get_file_size(filename):
    return Path(filename).stat().st_size


if "size" not in df.keys():
    print("\Computing file size")
    df["size"] = df["fullpath"].parallel_apply(get_file_size)
    df.to_csv(output_filename, sep="\t", index=False)

pprint("Get mime-type")
import magic


def get_mime_type(filename):
    mime = magic.Magic(mime=True)
    return mime.from_file(filename)


if "mime-type" not in df.keys():
    print("\nComputing mime-type")
    df["mime-type"] = df["fullpath"].parallel_apply(get_mime_type)
    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Get download link for each file")


def get_url(filename):
    filename = str(filename)
    return filename.replace("/bil/data/", "https://download.brainimagelibrary.org/")


if "download_url" not in df.keys():
    print("\nComputing download URL")
    df["download_url"] = df["fullpath"].parallel_apply(get_url)
    df.to_csv(output_filename, sep="\t", index=False)

import shutil

# Compute md5 checksums
import warnings

warnings.filterwarnings("ignore")

###############################################################################################################
pprint("Computing MD5 checksum")

import warnings

warnings.filterwarnings("ignore")


def __compute_md5sum(filename):
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

    md5 = hashlib.md5()

    if Path(filename).is_file() or Path(filename).is_symlink():
        with open(filename, "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)

    return md5.hexdigest()


def __get_chunk_size(dataframe):
    if len(dataframe) < 1000:
        return 10
    elif len(dataframe) < 10000:
        return 100
    elif len(dataframe) < 100000:
        return 250
    elif len(dataframe) < 500000:
        return 500
    else:
        return 500


if len(df) < 100:
    if "md5" in df.keys():
        files = df[df["md5"].isnull()]
    else:
        files = df
    print(f"Number of files to process is {str(len(files))}")

    if len(files) > 0:
        files["md5"] = files["full_path"].parallel_apply(__compute_md5sum)
        df = __update_dataframe(df, files, "md5")
        df.to_csv(output_filename, sep="\t", index=False)
else:
    if "md5" in df.keys():
        files = df[df["md5"].isnull()]
    else:
        files = df

    if len(files) != 0:
        n = __get_chunk_size(files)
        print(f"Number of files to process is {str(len(files))}")
        if n < 25:
            files["md5"] = files["full_path"].parallel_apply(__compute_md5sum)
            df = __update_dataframe(df, files, "md5")
            df.to_csv(output_filename, sep="\t", index=False)
        else:
            chunks = np.array_split(files, n)
            chunk_counter = 1
            for chunk in chunks:
                print(f"\nProcessing chunk {str(chunk_counter)} of {str(len(chunks))}")
                chunk["md5"] = chunk["full_path"].parallel_apply(__compute_md5sum)
                df = __update_dataframe(df, chunk, "md5")
                chunk_counter = chunk_counter + 1

                if chunk_counter % 10 == 0 or chunk_counter == len(chunks):
                    print("\nSaving chunks to disk")
                    df.to_csv(output_filename, sep="\t", index=False)
    else:
        print("No files left to process")

df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Computing SHA256 checksum")


def __compute_sha256sum(filename):
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

    sha256 = hashlib.sha256()
    if Path(filename).is_file() or Path(filename).is_symlink():
        with open(filename, "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha256.update(data)

    return sha256.hexdigest()


def __get_chunk_size(dataframe):
    if len(dataframe) < 1000:
        return 10
    elif len(dataframe) < 10000:
        return 100
    elif len(dataframe) < 100000:
        return 250
    elif len(dataframe) < 500000:
        return 500
    else:
        return 500


if len(df) < 100:
    if "sha256" in df.keys():
        files = df[df["sha256"].isnull()]
    else:
        files = df
    print(f"Number of files to process is {str(len(files))}")

    if len(files) > 0:
        files["sha256"] = files["full_path"].parallel_apply(__compute_sha256sum)
        df = __update_dataframe(df, files, "sha256")
        df.to_csv(output_filename, sep="\t", index=False)
else:
    if "sha256" in df.keys():
        files = df[df["sha256"].isnull()]
    else:
        files = df

    if not files.empty:
        n = __get_chunk_size(files)
        print(f"Number of files to process is {str(len(files))}")

        if n < 25:
            files["sha256"] = files["full_path"].parallel_apply(__compute_sha256sum)
            df = __update_dataframe(df, files, "sha256")
            df.to_csv(output_filename, sep="\t", index=False)
        else:
            chunks = np.array_split(files, n)

            chunk_counter = 1
            for chunk in chunks:
                print(f"\nProcessing chunk {str(chunk_counter)} of {str(len(chunks))}")
                chunk["sha256"] = chunk["full_path"].parallel_apply(__compute_sha256sum)
                df = __update_dataframe(df, chunk, "sha256")
                chunk_counter = chunk_counter + 1

                if chunk_counter % 10 == 0 or chunk_counter == len(chunks):
                    print("\nSaving chunks to disk")
                    df.to_csv(output_filename, sep="\t", index=False)
    else:
        print("No files left to process")

df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Computing dataset level statistics")
import humanize


def get_url(filename):
    return filename.replace("/bil/data/", "https://download.brainimagelibrary.org/")


from numpyencoder import NumpyEncoder


def generate_dataset_uuid(directory):
    if directory[-1] == "/":
        directory = directory[:-1]

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, directory))


def get_directory_creation_date(directory):
    ti_c = os.path.getctime(directory)
    c_ti = time.ctime(ti_c)

    return c_ti


from urllib import request

remote_url = "https://submit.brainimagelibrary.org/search/summarymetadata"
local_file = "/tmp/summarymetadata.csv"

if Path(local_file).exists():
    Path(local_file).unlink()
request.urlretrieve(remote_url, local_file)

dataset = {}
dataset["dataset_uuid"] = generate_dataset_uuid(directory)
dataset["creation_date"] = get_directory_creation_date(directory)
dataset["directory"] = file.replace("_", "/")
dataset["download_url"] = get_url(dataset["directory"])
dataset["number_of_files"] = len(df)
dataset["size"] = df["size"].sum()
dataset["pretty_size"] = humanize.naturalsize(dataset["size"], gnu=True)
dataset["frequencies"] = df["extension"].value_counts().to_dict()
dataset["file_types"] = df["filetype"].value_counts().to_dict()

metadata = pd.read_csv(local_file, sep=",")
if not metadata[metadata["bildirectory"] == directory].empty:
    dataset["modality"] = metadata[metadata["bildirectory"] == directory][
        "generalmodality"
    ].values[0]
    dataset["affiliation"] = metadata[metadata["bildirectory"] == directory][
        "affiliation"
    ].values[0]
    dataset["metadata_version"] = metadata[metadata["bildirectory"] == directory][
        "metadata_version"
    ].values[0]
    dataset["title"] = metadata[metadata["bildirectory"] == directory]["title"].values[
        0
    ]

print(dataset)

df["fullpath"] = df["fullpath"].astype(str)
files = df.to_dict("records")
dataset["manifest"] = files

output_filename = "json/" + generate_dataset_uuid(directory) + ".json"
with open(output_filename, "w") as ofile:
    json.dump(
        dataset, ofile, indent=4, sort_keys=True, ensure_ascii=False, cls=NumpyEncoder
    )
