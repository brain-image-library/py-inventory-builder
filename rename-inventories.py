import random
import xxhash
import argparse
import datetime
import gzip
import threading
from numpyencoder import NumpyEncoder
import hashlib
import json
import shutil
import humanize
import warnings
import mimetypes
import magic
import os
import os.path
import sys
import shutil
import subprocess
import time
import uuid
import warnings
from pathlib import Path
import hashlib
from concurrent.futures import ProcessPoolExecutor
import compress_json
import numpy as np
import pandas as pd
import tabulate
from pandarallel import pandarallel
from tqdm import tqdm

def __generate_dataset_uuid(directory):
    if directory[-1] == "/":
        directory = directory[:-1]

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, directory))

filename = 'summary_metadata.tsv'
df = pd.read_csv(filename, sep='\t', low_memory=False)

for index, datum in df.iterrows():
        bildid = datum['bildid']
        print(bildid)
        new_filename = f'/bil/data/inventory/{bildid}.json.gz'

        directory = datum['bildirectory']
        old_filename = f'/bil/data/inventory/{__generate_dataset_uuid(directory)}.json.gz'

        if Path(old_filename).exists() and Path(old_filename).is_file():
            Path(old_filename).rename(new_filename)
        print(f'mv -v {old_filename} {new_filename}')

        old_filename = old_filename.replace('.json.gz','.json')
        if Path(old_filename).exists() and Path(old_filename).is_file():
            Path(old_filename).unlink()
        print(f'rm -fv {old_filename}')
