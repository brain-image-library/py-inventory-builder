import pandas as pd
from tqdm import tqdm
from pathlib import Path
import uuid
import subprocess
import json
from typing import List
from typing import Any


def get_files(directory: str, extension: str = "json") -> List[Path]:
    """
    Get a list of files with a specific extension in a directory.

    This function searches for files with a specified extension in the given directory
    and returns a list of `Path` objects representing the file paths.

    Parameters:
        directory (str):
            The directory path to search for files.
        extension (str, optional):
            The extension of files to search for (default is "json").

    Returns:
        List[Path]:
            A list of `Path` objects representing the files with the specified extension
            in the directory.

    Example:
        >>> files = get_files("/bil/data/inventory", extension="json")
        >>> for file_path in files:
        ...     print(file_path)

    :param directory: The directory path to search for files.
    :type directory: str
    :param extension: The extension of files to search for (default is "json").
    :type extension: str, optional
    :return: A list of `Path` objects representing the files with the specified extension
             in the directory.
    :rtype: List[Path]
    """
    files = list(Path(directory).glob(f"*.{extension}"))
    return files


def __get_metadata(file: str) -> Any:
    """
    Get metadata from a JSON file, excluding the 'manifest' field.

    This function extracts metadata from a JSON file while excluding the 'manifest'
    field. The resulting metadata is returned as a Python object.

    Parameters:
        file (str):
            The path to the input JSON file.

    Returns:
        Any:
            The extracted metadata as a Python object.

    Example:
        >>> metadata = __get_metadata("data.json")
        >>> print(metadata)

    :param file: The path to the input JSON file.
    :type file: str
    :return: The extracted metadata as a Python object.
    :rtype: Any
    """
    temp_file = f"/tmp/{uuid.uuid4()}.json"
    command = f"cat {file} | jq 'del(.manifest)' > {temp_file}"
    subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    try:
        with open(temp_file, "r") as file:
            data = file.read()
            data = json.loads(data)

        if Path(temp_file).exists():
            Path(temp_file).unlink()

        return data
    except:
        print(temp_file)
        return {}


def __get_manifest(file: str) -> Any:
    """
    Get the 'manifest' field from a JSON file.

    This function extracts the 'manifest' field from a JSON file and returns it as
    a Python object.

    Parameters:
        file (str):
            The path to the input JSON file.

    Returns:
        Any:
            The extracted 'manifest' field as a Python object.

    Example:
        >>> manifest = __get_manifest("data.json")
        >>> print(manifest)

    :param file: The path to the input JSON file.
    :type file: str
    :return: The extracted 'manifest' field as a Python object.
    :rtype: Any
    """
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


def __summarize(file):
    data = __get_manifest(file)
    df = pd.DataFrame(data)

    summary = {}
    summary["extension"] = df["extension"].value_counts().to_dict()
    summary["file_type"] = df["filetype"].value_counts().to_dict()
    summary["mime-type"] = df["mime-type"].value_counts().to_dict()
    summary["size"] = int(df["size"].sum())
    summary["number_of_files"] = len(df)

    return summary


def summarize(file):
    data = __get_metadata(file)
    data["manifest"] = __summarize(file)

    del data["file_types"]
    del data["frequencies"]
    del data["size"]
    del data["pretty_size"]
    del data["number_of_files"]

    return data


def main():
    """
    Main function to process files and generate summaries.

    This function processes each JSON file in the input directory, generates a summary,
    and saves the summary as a JSON file in the output directory.
    """

    input_directory = "/bil/data/inventory"
    output_directory = "summary"

    # Get a list of JSON files
    files = get_files(input_directory, "json")

    # Create the output directory if it doesn't exist
    if not Path(output_directory).exists():
        Path(output_directory).mkdir()

    # Process each file and create summaries
    for file in files:
        print(file)
        summary = summarize(file)
        output_file = f"{output_directory}/{file.name}"

        if Path(output_file).exists():
            Path(output_file).unlink()

        with open(output_file, "w") as json_file:
            json.dump(summary, json_file, indent=4)

        # Uncomment the break statement if you want to process only one file
        break


if __name__ == "__main__":
    main()
