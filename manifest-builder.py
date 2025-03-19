import sys


def safe_import(module_name):
    try:
        globals()[module_name] = __import__(module_name)
    except ImportError as e:
        print(
            f"Error: Failed to import '{module_name}'. Please install the required package."
        )
        sys.exit(1)


# List of modules to import
modules = [
    "random",
    "xxhash",
    "argparse",
    "datetime",
    "gzip",
    "threading",
    "numpyencoder",
    "hashlib",
    "json",
    "shutil",
    "humanize",
    "warnings",
    "mimetypes",
    "magic",
    "os",
    "os.path",
    "sys",
    "shutil",
    "subprocess",
    "time",
    "uuid",
    "warnings",
    "pathlib",
    "hashlib",
    "concurrent.futures",
    "compress_json",
    "tabulate",
    "pandarallel",
    "tqdm",
    "zipfile",
]

# Attempt to import each module safely
for module in modules:
    safe_import(module)

import pandas as pd
import numpy as np

# Explicit imports for modules that use submodules
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from numpyencoder import NumpyEncoder
from pandarallel import pandarallel
from tqdm import tqdm


def pprint(msg):
    """
    Pretty-print the given message with horizontal lines above and below.

    Parameters:
        msg (str): The message to be pretty-printed.

    Returns:
        None

    Example:
        >>> pprint("Hello, World!")
        +-------------+
        |Hello, World!|
        +-------------+
    """

    row = len(msg)
    h = "".join(["+"] + ["-" * row] + ["+"])
    result = "\n" + h + "\n" "|" + msg + "|" "\n" + h
    print(result)


def __create_checkpoint_file(filename):
    """
    Creates a checkpoint file if it doesn't exist.

    Args:
        filename (str): The name of the file to create.

    Returns:
        bool: True if the file is created successfully, False if the file already exists.

    Raises:
        Any exceptions raised during file creation.

    Example:
        >>> __create_checkpoint_file("example.txt")
        Checkpoint file 'example.txt' created successfully.
        True
    """
    # Create a Path object for the specified filename
    file_path = Path(filename)

    # Check if the file already exists
    if not file_path.exists():
        # If the file doesn't exist, create an empty file
        with open(file_path, "w"):
            pass
        print(f"Checkpoint file '{filename}' created successfully.")
        return True
    else:
        print(f"Checkpoint file '{filename}' already exists.")
        return False


def __remove_checkpoint_file(filename):
    # Create a Path object for the specified filename
    print(f"Checking the existence of checkpoint file {filename}")
    file_path = Path(filename)

    # Check if the file exists
    if file_path.exists():
        # If the file exists, remove it
        file_path.unlink()
        print(f"Checkpoint file '{filename}' removed successfully.")
        return True
    else:
        print(f"Checkpoint file '{filename}' does not exist.")
        return False


def __get_file_size(filename):
    return Path(filename).stat().st_size


def __get_relative_path(full_path):
    answer = str(full_path).replace(f"{directory}/", "")
    return answer


def __update_dataframe(dataset, temp, key):
    for index, datum in temp.iterrows():
        dataset.loc[index, key] = temp.loc[index, key]
    return dataset


def __get_filename(filename):
    return Path(filename).stem + Path(filename).suffix


def __compute_xxh64sum(filename, file_extensions):
    # Check if the file extension is in the list to skip
    if any(filename.endswith(ext) for ext in file_extensions):
        return None

    hasher = xxhash.xxh64()  # Choose the appropriate hash function (xxh32, xxh64, etc.)
    BUFF_SIZE = 65536

    try:
        # Open the file in binary mode
        with open(filename, "rb") as file:
            for chunk in iter(lambda: file.read(BUFF_SIZE), b""):  # Read file in chunks
                hasher.update(chunk)  # Update hash with each chunk

        return hasher.hexdigest()  # Get the hexadecimal digest of the hash
    except Exception as e:
        # Print the filename and raise the exception
        print(f"Error processing file: {filename}: {e}")
        return None


def __compute_b2sum(filename, file_extensions):
    # Check if the file extension is in the list to skip
    if any(filename.endswith(ext) for ext in file_extensions):
        return None

    hash_blake2b = hashlib.blake2b()
    buffer_size = 8192  # Read the file in chunks of 8KB

    try:
        if Path(filename).is_file() or Path(filename).is_symlink():
            with open(filename, "rb") as f:
                while chunk := f.read(buffer_size):
                    hash_blake2b.update(chunk)

        return hash_blake2b.hexdigest()
    except Exception as e:
        print(f"Unable to process {filename}: {e}")
        return None


def __compute_sha256sum(filename, file_extensions):
    # Check if the file extension is in the list to skip
    if any(filename.endswith(ext) for ext in file_extensions):
        return None

    BUF_SIZE = 65536  # Read in 64KB chunks
    sha256 = hashlib.sha256()

    if Path(filename).is_file() or Path(filename).is_symlink():
        with open(filename, "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha256.update(data)

    return sha256.hexdigest()


def __compute_sha256sum_threaded(filename, file_extensions):
    BUF_SIZE = 65536

    # Check if the file extension is in the list to skip
    if any(filename.endswith(ext) for ext in file_extensions):
        return None

    sha256 = hashlib.sha256()

    if Path(filename).is_file() or Path(filename).is_symlink():

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

    return sha256.hexdigest()


def __compute_md5sum_threaded(filename):
    BUF_SIZE = 65536

    md5 = hashlib.md5()

    if Path(filename).is_file() or Path(filename).is_symlink():

        def hash_chunk(chunk, md5):
            md5.update(chunk)

        threads = []
        with open(filename, "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                thread = threading.Thread(target=hash_chunk, args=(data, md5))
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()

    return md5.hexdigest()


def __compute_md5sum(filename, file_extensions):
    # Check if the file extension is in the list to skip
    if any(filename.endswith(ext) for ext in file_extensions):
        return None

    BUF_SIZE = 65536  # Read in 64KB chunks
    md5 = hashlib.md5()

    if Path(filename).is_file() or Path(filename).is_symlink():
        with open(filename, "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)

    return md5.hexdigest()


def __clean_directory(directory):
    if directory[-1] == "/":
        directory = directory[:-1]
    return directory


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


def __generate_dataset_uuid(directory):
    if directory[-1] == "/":
        directory = directory[:-1]

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, directory))


def __get_directory_creation_date(directory):
    ti_c = os.path.getctime(directory)
    c_ti = time.ctime(ti_c)

    return c_ti


def __get_files(directory):
    files = subprocess.check_output(["lfs", "find", "-type", "f", directory])
    files = str(files)
    files = files.replace("b'/bil/data/", "/bil/data/")
    files = files.split("\\n")
    files = files[:-1]
    return files


def __get_file_extension(filename):
    if Path(filename).is_file() or Path(filename).is_symlink():
        extension = Path(filename).suffix
        if extension == ".tiff" or extension == ".tif":
            if str(filename).find("ome.tif") > 0:
                extension = ".ome.tif"

    return extension


def __get_number_of_files(directory):
    return len(__get_files(directory))


def __get_mime_type(filename):
    answer = mimetypes.guess_type(filename)
    try:
        return answer[0]
    except:
        return None


def __get_filetype(extension):
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

    tracings = {".swc", ".marker"}
    if extension in tracings:
        return "tracing"

    return "other"


def __get_url(filename):
    filename = str(filename)
    return filename.replace("/bil/data/", "https://download.brainimagelibrary.org/")


def __get_file_creation_date(filename):
    t = os.path.getmtime(str(filename))
    return str(datetime.datetime.fromtimestamp(t))


####################################################################################
def __to_json(df, directory):
    df["fullpath"] = df["fullpath"].astype(str)
    files = df.to_dict("records")
    dataset["manifest"] = files

    output_filename = "json/" + __generate_dataset_uuid(directory) + ".json"
    with open(output_filename, "w") as ofile:
        json.dump(
            dataset,
            ofile,
            indent=4,
            sort_keys=True,
            ensure_ascii=False,
            cls=NumpyEncoder,
        )

    if compress_json_file_on_bil_data:
        if compress_json_file_on_bil_data:
            with gzip.open(f"{output_filename}.gz", "wt") as f:
                f.write(str(dataset))

    print(f"Saving results to {output_filename}.")

    if update_json_file_on_bil_data:
        output_filename = (
            f"/bil/data/inventory/datasets/{__generate_dataset_uuid(directory)}.json"
        )
        with open(output_filename, "w") as ofile:
            json.dump(
                dataset,
                ofile,
                indent=4,
                sort_keys=True,
                ensure_ascii=False,
                cls=NumpyEncoder,
            )

    print(f"Updating file {output_filename}.")

    if compress_json_file_on_bil_data:
        with gzip.open(f"{output_filename}.gz", "wt") as f:
            f.write(str(dataset))


####################################################################################
def __to_json(df, bildid):
    df["fullpath"] = df["fullpath"].astype(str)
    files = df.to_dict("records")
    dataset["manifest"] = files

    output_filename = f"json/{bildid}.json"
    with open(output_filename, "w") as ofile:
        json.dump(
            dataset,
            ofile,
            indent=4,
            sort_keys=True,
            ensure_ascii=False,
            cls=NumpyEncoder,
        )

    if compress_json_file_on_bil_data:
        if compress_json_file_on_bil_data:
            with gzip.open(f"{output_filename}.gz", "wt") as f:
                f.write(str(dataset))

    print(f"Saving results to {output_filename}.")

    if update_json_file_on_bil_data:
        output_filename = (
            f"/bil/data/inventory/datasets/{__generate_dataset_uuid(directory)}.json"
        )
        with open(output_filename, "w") as ofile:
            json.dump(
                dataset,
                ofile,
                indent=4,
                sort_keys=True,
                ensure_ascii=False,
                cls=NumpyEncoder,
            )

    print(f"Updating file {output_filename}.")

    if compress_json_file_on_bil_data:
        with gzip.open(f"{output_filename}.gz", "wt") as f:
            f.write(str(dataset))


#####################################################################################
def __to_metadata(data, bildid):
    # Create the "zip" directory if it does not exist
    zip_dir = Path.cwd() / "metadata"
    zip_dir.mkdir(parents=True, exist_ok=True)

    # Define temporary TSV filename and ZIP file paths
    output_filename = zip_dir / f"{bildid}.json"

    with open(output_filename, "w") as temp_file:
        json.dump(data, temp_file, indent=4)

    print(f"Saved metadata TSV file as JSON to {output_filename}.")


#####################################################################################
def __to_zip(df, bildid, directory):
    """
    Save a DataFrame to a ZIP file containing a TSV file.

    Parameters:
    - df (pd.DataFrame): The DataFrame to save.
    - bildid (str): Identifier for the ZIP and TSV files.
    - directory (str): Base directory where the ZIP files are stored.
    """
    # Ensure "fullpath" column is of string type
    df["fullpath"] = df["fullpath"].astype(str)

    # Create the "zip" directory if it does not exist
    zip_dir = Path.cwd() / "zip"
    zip_dir.mkdir(parents=True, exist_ok=True)

    # Define temporary TSV filename and ZIP file paths
    temp_filename = zip_dir / f"{bildid}.tsv"
    output_filename = zip_dir / f"{bildid}.zip"

    # Save the DataFrame as a TSV file
    df.to_csv(temp_filename, sep="\t", index=False)
    print(f"Saved temporary TSV file to {temp_filename}.")

    # Compress the TSV file into a ZIP archive
    with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(temp_filename, arcname=f"{bildid}.tsv")
    print(f"Saved ZIP file to {output_filename}.")

    # Clean up the temporary TSV file
    # temp_filename.unlink()
    # print(f"Deleted temporary TSV file: {temp_filename}.")


###############################################################################################################
# Load file with files on directory
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", dest="directory", help="Directory")
parser.add_argument(
    "-n", "--number-of-cores", dest="ncores", help="Number of cores", default=2
)
parser.add_argument(
    "--update", action=argparse.BooleanOptionalAction, dest="update", default=False
)
parser.add_argument(
    "--compress", action=argparse.BooleanOptionalAction, dest="compress", default=False
)
parser.add_argument(
    "--remove-checkpoints",
    action=argparse.BooleanOptionalAction,
    dest="remove_checkpoints",
    default=False,
)
parser.add_argument(
    "--rebuild", action=argparse.BooleanOptionalAction, dest="rebuild", default=False
)
parser.add_argument(
    "--avoid-checksums",
    action=argparse.BooleanOptionalAction,
    dest="avoid_checksums",
    default=False,
)
parser.add_argument(
    "--multi-threading",
    action=argparse.BooleanOptionalAction,
    dest="multi_threading",
    default=False,
)
args = parser.parse_args()
directory = args.directory
update_json_file_on_bil_data = bool(args.update)
compress_json_file_on_bil_data = bool(args.compress)
avoid_checksums = bool(args.avoid_checksums)
remove_checkpoints = bool(args.remove_checkpoints)
multi_threading = bool(args.multi_threading)
rebuild = bool(args.rebuild)
ncores = int(args.ncores)
pandarallel.initialize(progress_bar=True, nb_workers=ncores)

if directory[-1] == "/":
    directory = directory[:-1]

if not Path(".data").exists():
    Path(".data").mkdir()

if not Path("json").exists():
    Path("json").mkdir()

file = directory.replace("/", "_")
output_filename = f".data/{file}.tsv"

file_extensions = ["ims"]
print(f"Ignoring files with extensions {file_extensions}")

checkpoint = output_filename.replace(".tsv", ".computing")
if not __create_checkpoint_file(checkpoint):
    print("Another process is building an inventory for this dataset. Exiting.")
    sys.exit()

done = output_filename.replace(".tsv", ".done")

if Path(done).exists() and remove_checkpoints:
    print("Removing checkpoints")
    Path(checkpoint).unlink()
    Path(done).unlink()

if Path(done).exists():
    print("The processing of this dataset is finished. Exiting.")
    Path(checkpoint).unlink()
    sys.exit()

if rebuild:
    if Path(output_filename).exists() or Path(output_filename).is_symlink():
        print("Rebuilding dataframe. Removing existing TSV file.")
        Path(output_filename).unlink()

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
    files = __get_files(directory)
    df = pd.DataFrame()
    df["fullpath"] = files

df.to_csv(output_filename, sep="\t", index=False)

pprint(f"Processing dataset in {directory}")

if df.empty:
    print(f"No files found in {directory}. Exiting program.")
    sys.exit()

###############################################################################################################
pprint("Get file extensions")
if "extension" not in df.keys():
    print("Computing file extension")
    df["extension"] = df["fullpath"].parallel_apply(__get_file_extension)
    df.to_csv(output_filename, sep="\t", index=False)
else:
    print("No files left to process.")

###############################################################################################################
pprint("Get filename")
if "filename" not in df.keys():
    df["filename"] = df["fullpath"].parallel_apply(__get_filename)
    df.to_csv(output_filename, sep="\t", index=False)
else:
    print("No files left to process.")

###############################################################################################################
pprint("Get relative path")
if "relativepath" not in df.keys():
    print("Computing relative paths")
    df["relativepath"] = df["fullpath"].parallel_apply(__get_relative_path)
    df.to_csv(output_filename, sep="\t", index=False)
else:
    print("No files left to process.")

###############################################################################################################
pprint("Get file type")
if "filetype" not in df.keys():
    print("Computing file type")
    df["filetype"] = df["extension"].parallel_apply(__get_filetype)
    df.to_csv(output_filename, sep="\t", index=False)
else:
    print("No files left to process.")

###############################################################################################################
pprint("Get file creation date")
if "modification_time" not in df.keys():
    print("Computing modification time")
    df["modification_time"] = df["fullpath"].parallel_apply(__get_file_creation_date)
    df.to_csv(output_filename, sep="\t", index=False)
else:
    print("No files left to process.")

###############################################################################################################
pprint("Get file size")
if "size" not in df.keys():
    print("Computing file size")
    df["size"] = df["fullpath"].parallel_apply(__get_file_size)
    df.to_csv(output_filename, sep="\t", index=False)
else:
    print("No files left to process.")

###############################################################################################################
pprint("Get mime-type")
if "mime-type" not in df.keys():
    print("Computing mime-type")
    df["mime-type"] = df["fullpath"].parallel_apply(__get_mime_type)
    df.to_csv(output_filename, sep="\t", index=False)
else:
    print("No files left to process.")

###############################################################################################################
pprint("Get download link for each file")
if "download_url" not in df.keys():
    print("Computing download URL")
    df["download_url"] = df["fullpath"].parallel_apply(__get_url)
    df.to_csv(output_filename, sep="\t", index=False)
else:
    print("No files left to process.")

###############################################################################################################
warnings.filterwarnings("ignore")
if not avoid_checksums:
    pprint("Computing MD5 checksum")

    if len(df) < 100:
        if "md5" in df.keys():
            files = df[df["md5"].isnull()]
        else:
            files = df
        print(f"Number of files to process is {str(len(files))}.")

        if len(files) > 0:
            if multi_threading:
                files["md5"] = files["fullpath"].parallel_apply(
                    __compute_md5sum_threaded
                )
            else:
                files["md5"] = files["fullpath"].parallel_apply(lambda x: __compute_md5sum(x, file_extensions))

            df = __update_dataframe(df, files, "md5")
            df.to_csv(output_filename, sep="\t", index=False)
    else:
        if "md5" in df.keys():
            files = df[df["md5"].isnull()]
        else:
            files = df

        if len(files) != 0:
            n = __get_chunk_size(files)
            print(f"Number of files to process is {str(len(files))}.")
            if n < 25:
                if multi_threading:
                    files["md5"] = files["fullpath"].parallel_apply(
                        __compute_md5sum_threaded
                    )
                else:
                    files["md5"] = files["fullpath"].parallel_apply(lambda x: __compute_md5sum(x, file_extensions))

                df = __update_dataframe(df, files, "md5")
                df.to_csv(output_filename, sep="\t", index=False)
            else:
                chunks = np.array_split(files, n)
                chunk_counter = 1
                for chunk in chunks:
                    print(
                        f"\nProcessing chunk {str(chunk_counter)} of {str(len(chunks))}"
                    )
                    if multi_threading:
                        chunk["md5"] = chunk["fullpath"].parallel_apply(
                            __compute_md5sum_threaded
                        )
                    else:
                        chunk["md5"] = chunk["fullpath"].parallel_apply(lambda x: __compute_md5sum(x, file_extensions))

                    df = __update_dataframe(df, chunk, "md5")
                    chunk_counter = chunk_counter + 1

                    if chunk_counter % 10 == 0 or chunk_counter == len(chunks):
                        print("\nSaving chunks to disk")
                        df.to_csv(output_filename, sep="\t", index=False)
        else:
            print("No files left to process.")

    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
warnings.filterwarnings("ignore")
if not avoid_checksums:
    pprint("Computing xxh64 checksum")

    if len(df) < 100:
        if "xxh64" in df.keys():
            files = df[df["xxh64"].isnull()]
        else:
            files = df
        print(f"Number of files to process is {str(len(files))}.")

        if len(files) > 0:
            files["xxh64"] = files["fullpath"].parallel_apply(__compute_xxh64sum)

            df = __update_dataframe(df, files, "xxh64")
            df.to_csv(output_filename, sep="\t", index=False)
    else:
        if "xxh64" in df.keys():
            files = df[df["xxh64"].isnull()]
        else:
            files = df

        if len(files) != 0:
            n = __get_chunk_size(files)
            print(f"Number of files to process is {str(len(files))}.")
            if n < 25:
                files["xxh64"] = files["fullpath"].parallel_apply(__compute_xxh64sum)

                df = __update_dataframe(df, files, "xxh64")
                df.to_csv(output_filename, sep="\t", index=False)
            else:
                chunks = np.array_split(files, n)
                chunk_counter = 1
                for chunk in chunks:
                    print(
                        f"\nProcessing chunk {str(chunk_counter)} of {str(len(chunks))}"
                    )
                    chunk["xxh64"] = chunk["fullpath"].parallel_apply(
                        __compute_xxh64sum
                    )

                    df = __update_dataframe(df, chunk, "xxh64")
                    chunk_counter = chunk_counter + 1

                    if chunk_counter % 10 == 0 or chunk_counter == len(chunks):
                        print("\nSaving chunks to disk")
                        df.to_csv(output_filename, sep="\t", index=False)
        else:
            print("No files left to process.")

    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
warnings.filterwarnings("ignore")
if not avoid_checksums:
    pprint("Computing b2sum checksum")

    if len(df) < 100:
        if "b2sum" in df.keys():
            files = df[df["b2sum"].isnull()]
        else:
            files = df
        print(f"Number of files to process is {str(len(files))}.")

        if len(files) > 0:
            files["b2sum"] = files["fullpath"].parallel_apply(__compute_b2sum)

            df = __update_dataframe(df, files, "b2sum")
            df.to_csv(output_filename, sep="\t", index=False)
    else:
        if "b2sum" in df.keys():
            files = df[df["b2sum"].isnull()]
        else:
            files = df

        if len(files) != 0:
            n = __get_chunk_size(files)
            print(f"Number of files to process is {str(len(files))}.")
            if n < 25:
                files["b2sum"] = files["fullpath"].parallel_apply(__compute_b2sum)

                df = __update_dataframe(df, files, "b2sum")
                df.to_csv(output_filename, sep="\t", index=False)
            else:
                chunks = np.array_split(files, n)
                chunk_counter = 1
                for chunk in chunks:
                    print(
                        f"\nProcessing chunk {str(chunk_counter)} of {str(len(chunks))}"
                    )
                    chunk["b2sum"] = chunk["fullpath"].parallel_apply(__compute_b2sum)

                    df = __update_dataframe(df, chunk, "b2sum")
                    chunk_counter = chunk_counter + 1

                    if chunk_counter % 10 == 0 or chunk_counter == len(chunks):
                        print("\nSaving chunks to disk")
                        df.to_csv(output_filename, sep="\t", index=False)
        else:
            print("No files left to process.")

    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
if not avoid_checksums:
    pprint("Computing SHA256 checksum")

    if len(df) < 100:
        if "sha256" in df.keys():
            files = df[df["sha256"].isnull()]
        else:
            files = df
        print(f"Number of files to process is {str(len(files))}.")

        if len(files) > 0:
            if multi_threading:
                files["sha256"] = files["fullpath"].parallel_apply(
                    __compute_sha256sum_threaded
                )
            else:
                files["sha256"] = files["fullpath"].parallel_apply(__compute_sha256sum)

            df = __update_dataframe(df, files, "sha256")
            df.to_csv(output_filename, sep="\t", index=False)
    else:
        if "sha256" in df.keys():
            files = df[df["sha256"].isnull()]
        else:
            files = df

        if not files.empty:
            n = __get_chunk_size(files)
            print(f"Number of files to process is {str(len(files))}.")

            if n < 25:
                if multi_threading:
                    files["sha256"] = files["fullpath"].parallel_apply(
                        __compute_sha256sum_threaded
                    )
                else:
                    files["sha256"] = files["fullpath"].parallel_apply(
                        __compute_sha256sum
                    )

                df = __update_dataframe(df, files, "sha256")
                df.to_csv(output_filename, sep="\t", index=False)
            else:
                chunks = np.array_split(files, n)

                chunk_counter = 1
                for chunk in chunks:
                    print(
                        f"\nProcessing chunk {str(chunk_counter)} of {str(len(chunks))}"
                    )
                    if multi_threading:
                        chunk["sha256"] = chunk["fullpath"].parallel_apply(
                            __compute_sha256sum_threaded
                        )
                    else:
                        chunk["sha256"] = chunk["fullpath"].parallel_apply(
                            __compute_sha256sum
                        )

                    df = __update_dataframe(df, chunk, "sha256")
                    chunk_counter = chunk_counter + 1

                    if chunk_counter % 10 == 0 or chunk_counter == len(chunks):
                        print("\nSaving chunks to disk")
                        df.to_csv(output_filename, sep="\t", index=False)
        else:
            print("No files left to process.")

    df.to_csv(output_filename, sep="\t", index=False)

###############################################################################################################
pprint("Computing dataset level statistics")

dataset = {}
dataset["dataset_uuid"] = __generate_dataset_uuid(directory)
dataset["creation_date"] = __get_directory_creation_date(directory)
dataset["directory"] = directory
dataset["download_url"] = __get_url(dataset["directory"])
dataset["number_of_files"] = int(len(df))
dataset["size"] = int(df["size"].sum())
dataset["pretty_size"] = humanize.naturalsize(dataset["size"], gnu=True)
dataset["frequencies"] = df["extension"].value_counts().to_dict()
dataset["file_types"] = df["filetype"].value_counts().to_dict()

local_file = "summary_metadata.tsv"
metadata = pd.read_csv(local_file, sep="\t")

metadata["bildirectory"] = metadata["bildirectory"].apply(__clean_directory)

if not metadata[metadata["bildirectory"] == directory].empty:
    dataset["status"] = "Published"

    dataset["modality"] = metadata[metadata["bildirectory"] == directory][
        "generalmodality"
    ].values[0]

    dataset["contributor_name"] = metadata[metadata["bildirectory"] == directory][
        "contributor"
    ].values[0]

    dataset["bildid"] = metadata[metadata["bildirectory"] == directory][
        "bildid"
    ].values[0]

    dataset["award_number"] = metadata[metadata["bildirectory"] == directory][
        "award_number"
    ].values[0]

    dataset["version"] = metadata[metadata["bildirectory"] == directory][
        "metadata_version"
    ].values[0]

    dataset["species"] = metadata[metadata["bildirectory"] == directory][
        "species"
    ].values[0]

    dataset["taxonomy"] = metadata[metadata["bildirectory"] == directory][
        "taxonomy"
    ].values[0]

    dataset["technique"] = metadata[metadata["bildirectory"] == directory][
        "technique"
    ].values[0]

__to_json(df, dataset["bildid"])
__to_metadata(dataset, dataset["bildid"])
__to_zip(df, dataset["bildid"], directory)
__remove_checkpoint_file(checkpoint)
__create_checkpoint_file(done)
