import hashlib
import pandas as pd
from tqdm import tqdm
import threading
from pathlib import Path
import time
import argparse
import subprocess
from pandarallel import pandarallel


def compute_sha256sum_threaded(filename):
    BUF_SIZE = 65536

    sha256 = hashlib.sha256()

    if Path(filename).is_file() or Path(filename).is_symlink():
        start_time = time.time()  # Measure start time

        def hash_chunk(chunk, sha256):
            sha256.update(chunk)

        threads = []
        with open(filename, "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                thread = threading.Thread(target=hash_chunk, args=(data, sha256))
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()

        end_time = time.time()  # Measure end time
        elapsed_time = end_time - start_time  # Calculate elapsed time

        return {
            "filename": filename,
            "threads": len(threads),
            "elapsed_time": elapsed_time,
            "sha256": sha256.hexdigest(),
        }

    return {"filename": None, "threads": 0, "elapsed_time": 0, "sha256": None}


def __get_files(directory):
    files = subprocess.check_output(["lfs", "find", "-type", "f", directory])
    files = str(files)
    files = files.replace("b'/bil/data/", "/bil/data/")
    files = files.split("\\n")
    files = files[:-1]
    return files


# Load file with files on directory
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", dest="directory", help="Directory")

args = parser.parse_args()
directory = args.directory

ncores = 1
pandarallel.initialize(progress_bar=True, nb_workers=ncores)

files = __get_files(directory)

data = []
for file in tqdm(files):
    data.append(compute_sha256sum_threaded(file))

df = pd.DataFrame(data)
file = "sha256.tsv"
df.to_csv(file, sep="\t", index=False, mode="a", header=not df.empty)
