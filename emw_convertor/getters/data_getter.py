import logging
import os
from typing import Optional

import filetype
import pandas as pd
from google.cloud import storage

from emw_convertor import config, local_data_input_path

logger = logging.getLogger("<Bilstein SLExA ETL>")


def is_valid_format(dir_path: str, file_name: str) -> bool:

    accepted_file_types = config["file_types"]["valid_file_extensions"]

    # Detect file type
    kind = filetype.guess(os.path.join(dir_path, file_name))
    if kind is None:  # Check if the file has a valid type and extension
        print(
            f"File{file_name} IS NOT A VALID FILE TYPE IN {dir_path}. Check config.yaml for more info."
        )
        return False

    # Validate file extension and MIME type
    is_valid_extension = kind.extension in accepted_file_types.keys()
    is_valid_mime = kind.mime in accepted_file_types.values()

    return is_valid_extension and is_valid_mime


def generate_path_list(folder_name) -> list:
    """
    Checks if the file at the specified path has a valid format based on allowed file types
    defined in the configuration file.

    Args:
        dir_path (str): Directory path where the file is located.
        file_name (str): Name of the file to validate.
        config_path (str): Path to the configuration file with valid file extensions and MIME types.

    Returns:
        bool: True if the file is of a valid type and format, otherwise False.
    """
    valid_file_list = []
    if config["etl_pipeline"]["load_local"]:
        dir_path = os.path.join(local_data_input_path, folder_name)
        try:
            for file_name in os.listdir(dir_path):
                if os.path.isfile(
                    os.path.join(dir_path, file_name)
                ) and is_valid_format(dir_path, file_name):
                    # add filename to list
                    valid_file_list.append(os.path.join(dir_path, file_name))
            return valid_file_list
        except IndexError:
            return None
    else:
        # reserved for loading files from Google cloud environment
        # write a function to load list of files from Google storage. In future we do not need this function
        # while each pipline will run only one file usinf Prefect orchestrator
        pass


def load_excel_file(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load an Excel file from a local path or a Google Cloud Storage path.

    Args:
        file_path (str): Path to the Excel file. Can be a local path or a GCS URL (gs://).

    Returns:
        Optional[pd.DataFrame]: The loaded DataFrame if the file is valid, otherwise None.
    """
    if config["etl_pipeline"]["load_local"]:
        logger.info("The source file is loading from local repository")
        return load_from_local(file_path)
    else:
        logger.info("The source file is loading from Google storage")
        return load_from_gcs(file_path)


def load_from_local(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load an Excel file from a local path.

    Args:
        file_path (str): Local path to the Excel file.

    Returns:
        Optional[pd.DataFrame]: The loaded DataFrame if successful, otherwise None.
    """

    if os.path.exists(file_path):
        df = pd.read_excel(file_path, header=None)
        logger.info(f"DataFrame is created successfully with the shape:{df.shape}")

        # Immediately sanitize the DataFrame to prevent NaN issues downstream
        # Replace all NaN values with None to prevent JSON serialization errors
        import numpy as np

        df = df.replace(
            [
                pd.NA,
                pd.NaT,
                float("nan"),
                float("inf"),
                -float("inf"),
                np.nan,
                np.inf,
                -np.inf,
            ],
            None,
        )

        logger.info("DataFrame sanitized to prevent NaN-related JSON errors")
        return df
    else:
        logger.error(f"Invalid file type or path: {file_path}")
        return None


def load_from_gcs(gcs_path: str) -> Optional[pd.DataFrame]:
    """
    Load an Excel file from Google Cloud Storage.

    Args:
        gcs_path (str): The GCS URL of the file (e.g., gs://bucket_name/file.xlsx).

    Returns:
        Optional[pd.DataFrame]: The loaded DataFrame if successful, otherwise None.
    """
    try:
        client = storage.Client()
        bucket_name, file_name = gcs_path[5:].split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        temp_local_path = f"/tmp/{file_name}"
        blob.download_to_filename(temp_local_path)
        return pd.read_excel(temp_local_path)
    except Exception as e:
        logger.error(f"Error loading file from GCS: {e}")
        return None
