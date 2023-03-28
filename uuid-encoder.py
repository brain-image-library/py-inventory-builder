import argparse

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
args = parser.parse_args()
directory = args.directory

if directory[-1] == "/":
    directory = directory[:-1]

file = directory.replace("/", "_")
output_filename = file + ".tsv"

print(output_filename)
