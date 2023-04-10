import argparse
import uuid


def __pprint(msg):
    row = len(msg)
    h = "".join(["+"] + ["-" * row] + ["+"])
    result = h + "\n" "|" + msg + "|" "\n" + h
    print(result)


def __generate_dataset_uuid(directory):
    if directory[-1] == "/":
        directory = directory[:-1]

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, directory))


###############################################################################################################
# Load file with files on directory
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", dest="directory", help="Directory")
args = parser.parse_args()
directory = args.directory

print(__generate_dataset_uuid(directory))
