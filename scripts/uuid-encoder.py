import argparse
import uuid

def __pprint(msg):
    """
    Pretty prints a message with a surrounding border of '+' and '-' characters.

    Args:
        msg (str): The message to be printed.

    Returns:
        None
    """
    row = len(msg)
    h = "".join(["+"] + ["-" * row] + ["+"])
    result = h + "\n" "|" + msg + "|" "\n" + h
    print(result)

def __generate_dataset_uuid(directory):
    """
    Generates a UUID for a given directory path.

    The UUID is created using the DNS namespace and the directory path.

    Args:
        directory (str): The directory path for which UUID needs to be generated.

    Returns:
        str: A string representation of the generated UUID.
    """
    if directory.endswith("/"):
        directory = directory[:-1]

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, directory))

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Generate UUID for a specified directory")
    parser.add_argument("-d", "--directory", dest="directory", help="Directory path to generate UUID for")
    args = parser.parse_args()

    # Retrieve directory path from command-line arguments
    directory = args.directory

    if not directory:
        print("Error: Please provide a directory path using -d or --directory option.")
        return

    # Generate and print UUID for the specified directory
    uuid_value = __generate_dataset_uuid(directory)
    __pprint(f"UUID for directory '{directory}': {uuid_value}")

if __name__ == "__main__":
    main()