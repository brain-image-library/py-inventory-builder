import pandas as pd
from pathlib import Path


def get_files():
    directory = Path("/bil/data/inventory").glob("*.json")
    files = list(directory)
    return files


files = get_files()

import uuid
import subprocess
import json


def __get_metadata(file):
    temp_file = f"/tmp/{uuid.uuid4()}.json"
    command = f"cat {file} | jq 'del(.manifest)' > {temp_file}"
    subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    with open(temp_file, "r") as file:
        data = file.read()
        data = json.loads(data)

    if Path(temp_file).exists():
        Path(temp_file).unlink()

    return data


def __get_manifest(file):
    temp_file = f"/tmp/{uuid.uuid4()}.json"
    command = f"cat {file} | jq '.manifest' > {temp_file}"
    subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    with open(temp_file, "r") as file:
        data = file.read()
        data = json.loads(data)

    if Path(temp_file).exists():
        Path(temp_file).unlink()

    return data


file = files[3]


def __summarize(file):
    data = __get_manifest(file)

    summary = {}
    summary["extension"] = df["extension"].value_counts().to_dict()
    summary["file_type"] = df["filetype"].value_counts().to_dict()
    summary["mime-type"] = df["mime-type"].value_counts().to_dict()
    summary["size"] = df["size"].sum()
    summary["number_of_files"] = len(df)

    return summary


def summarize(file):
    data = __get_metadata(file)
    data["manifest"] = __summarize(file)

    data = data.drop("extension", axis=1)
    data = data.drop("filetype", axis=1)
    data = data.drop("mime-type", axis=1)
    data = data.drop("size", axis=1)
    data = data.drop("number_of_files", axis=1)

    return data


output_directory = "summary"
if not Path("summary").exists():
    Path("summary").mkdir()

from tqdm import tqdm

for file in tqdm(files):
    summary = summarize(file)
    df = pd.DataFrame(summary)
    output_file = f"{output_directory}/{file.name}"
    print(output_file)
    df.to_json(output_file, orient="records", lines=True, escape=False)
    break
