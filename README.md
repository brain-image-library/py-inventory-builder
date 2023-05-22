# File Directory Traverser and Manifest Builder

This Python script allows you to traverse a file directory and generate a file manifest containing useful information such as checksums and file sizes.

## Prerequisites

- Python 3.x installed on your system.

## Usage

1. Clone this repository.

2. Open a terminal or command prompt and navigate to the directory where the `manifest-builder.py` script is located.

3. Run the script by executing the following command:

```bash
D=/bil/data/collection/dataset
python ./manifest-builder.py -d $D
```
 
4. The script will recursively traverse the specified directory, generating a file manifest in JSON format.

5. A file manifest backup will be saved in the `.data` directory in the current working directory.

---
Copyright © 2020-2023 Pittsburgh Supercomputing Center. All Rights Reserved.

The [Biomedical Applications Group](https://www.psc.edu/biomedical-applications/) at the [Pittsburgh Supercomputing Center](http://www.psc.edu) in the [Mellon College of Science](https://www.cmu.edu/mcs/) at [Carnegie Mellon University](http://www.cmu.edu).
