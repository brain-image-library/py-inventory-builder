# Inventory Script

## Description

This Python script generates an inventory of files in a specified directory, including file metadata such as extension, file type, creation date, size, checksums, and more. The script can be configured to update existing inventories, compress JSON files, and utilize multi-threading for faster processing.

## Prerequisites

- Python 3
- Required Python packages can be installed using the following command:

```bash
pip install numpy pandas tabulate tqdm compress_json numpyencoder magic pandarallel
```

## Usage

```
python inventory_script.py -d <directory_path> [-n <number_of_cores>] [--update] [--compress] [--rebuild] [--avoid-checksums] [--multi-threading]
```

### Options

```
-d, --directory        Specify the target directory for inventory.
-n, --number-of-cores  Number of CPU cores to use for parallel processing.
--update               Update existing inventories.
--compress             Compress JSON files.
--rebuild              Rebuild the inventory, removing existing TSV file.
--avoid-checksums      Skip checksum computation.
--multi-threading      Enable multi-threading for faster processing.
```

## Example

```
python inventory_script.py -d /path/to/dataset -n 4 --update --compress
```

## License
This script is licensed under the MIT License.


---
Copyright © 2020-2023 Pittsburgh Supercomputing Center. All Rights Reserved.

The [Biomedical Applications Group](https://www.psc.edu/biomedical-applications/) at the [Pittsburgh Supercomputing Center](http://www.psc.edu) in the [Mellon College of Science](https://www.cmu.edu/mcs/) at [Carnegie Mellon University](http://www.cmu.edu).