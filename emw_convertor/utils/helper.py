import os
import pickle
import pandas as pd
import logging
from typing import List, Dict
import json
from emw_convertor import local_data_input_path

logger = logging.getLogger("<EMW SLExA ETL>")


def load_schema_list(schema_column: List[Dict], key: str) -> List[str]:
    """
    Load a specific value from a schema list of dictionaries.

    Args:
        schema_column (List[Dict]): A list of dictionaries representing the schema.
        key (str): The key to extract values for.

    Returns:
        List[str]: A list of extracted values corresponding to the key.

    Raises:
        ValueError: If the key is missing in the schema or the schema is invalid.
    """
    try:
        # Extract the values for the given key
        return [entry[key] for entry in schema_column if key in entry]
    except Exception as e:
        raise ValueError(f"Failed to load schema list for key '{key}': {e}") from e


def save_pickle_file(df: pd.DataFrame, file_name: str, folder="interim") -> None:
    """Save dataframe to pickle format

    Args:
        df (pd.DataFrame): Excel table converted to df
        file_name (str): the name of expected pickle file
    """
    try:
        with open(
            os.path.join(local_data_input_path, f"{folder}/{file_name}.pk"), "wb"
        ) as writer:
            pickle.dump(df, writer, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"file {file_name} is dumped in data folder successfully")
    except Exception as e:
        logging.error(f"Could not save pickel file :{e}")


def load_pickle_file(file_path: str) -> pd.DataFrame:
    """Load the pickle file inrto dataframe

    Args:
        file_name (str): pickle filename

    Returns:
        pd.DataFrame: Excel dataftame
    """
    try:
        with open(file_path, "rb") as f:
            data = pickle.load(f)
            if (
                not isinstance(data, dict)
                or "data_frame" not in data
                or "file_name" not in data
            ):
                raise ValueError(
                    "Pickle file must contain a dictionary with 'file_name' and 'data_frame' keys."
                )
        logging.info(f"Successfully loaded data from {file_path}")
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except pickle.PickleError:
        logging.error("Failed to load pickle file due to an unpickling error.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


def delete_file(file_path):
    """
    Deletes the specified file if it exists and logs the action.

    Args:
        file_path (str): The path of the file to be deleted.

    Raises:
        ValueError: If the provided path is not a file.
    """
    # Sanity check: Ensure the file path is valid
    if not isinstance(file_path, str):
        logging.error("Invalid input: file_path should be a string.")

    # Check if file exists
    if not os.path.exists(file_path):
        logging.warning(f"File not found: {file_path}. No action taken.")

    # Check if the path is a file and not a directory
    if not os.path.isfile(file_path):
        logging.error(f"The provided path is not a file: {file_path}")
        raise ValueError("The provided path is not a file.")

    try:
        # Attempt to delete the file
        os.remove(file_path)
        logging.info(f"File deleted successfully: {file_path}")
    except Exception as e:
        logging.error(f"Error deleting file {file_path}: {e}")


def delete_all_files(folder_path):
    """
    Delete all files in the specified folder.

    Args:
        folder_path (str): Path to the folder.

    Returns:
        None
    """
    try:
        # List all files in the folder
        files = [
            os.path.join(folder_path, file)
            for file in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, file))
        ]

        # Delete each file
        for file in files:
            os.remove(file)
            print(f"Deleted: {file}")

        print("All files have been deleted.")

    except FileNotFoundError:
        print(f"The folder '{folder_path}' does not exist.")
    except PermissionError:
        print(f"Permission denied to delete files in '{folder_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
