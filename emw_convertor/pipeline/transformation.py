import pandas as pd
import logging
import numpy as np

logger = logging.getLogger("<EMW SLExA ETL>")


def standardize_missing_values(df) -> pd.DataFrame:
    """
    Replace all common string representations of missing values with np.nan
    and ensure columns are in the correct numeric format where applicable.

    Args:
        df (pd.DataFrame): The input DataFrame to standardize.

    Returns:
        pd.DataFrame: DataFrame with standardized missing values.
    """
    # Replace common string representations of missing data with np.nan
    df.replace(["nan", "NaN", "N/A", "", "None"], np.nan, inplace=True)

    # Convert all columns to numeric if applicable, coercing errors to NaN
    # df = df.apply(lambda col: pd.to_numeric(col, errors='coerce') if col.dtype == 'object' else col)


def drop_rows_with_missing_values(df, threshold):
    """
    Drop rows in place from the DataFrame where 90% (or a specified percentage) of the required column values are empty.

    Args:
        df (pd.DataFrame): The input DataFrame.
        required_columns (list): List of required columns to check for missing values.
        threshold (float): The threshold percentage (0.9 means 90%).

    Returns:
        None: The function modifies the DataFrame in place.
    """

    required_columns = df.columns

    # Calculate the number of non-missing required columns needed to keep the row
    min_non_missing = int(len(required_columns) * (1 - threshold))

    # Filter rows based on the count of non-missing values in the required columns and drop them in place
    df.drop(
        df[df[required_columns].count(axis=1) <= min_non_missing].index, inplace=True
    )

    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    # Drop cols where all values in the row are NaN
    df = df.dropna(axis=1, how="all")  # axis=1 specifies col-wise operation

    # Drop rows where all values in the row are NaN
    df = df.dropna(axis=0, how="all")  # axis=0 specifies row-wise operation

    return df


def ensure_floating_point(df):
    try:
        # Replace commas with dots and convert to float
        df["thickness(mm)"] = (
            df["thickness(mm)"].astype(str).str.replace(",", ".").astype(float)
        )
        df["width(mm)"] = (
            df["width(mm)"].astype(str).str.replace(",", ".").astype(float)
        )
        logging.info("Ensured dimensions are formatted as floating-point numbers.")
        return df
    except ValueError as e:
        logging.error(f"ValueError - Invalid conversion to float: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during float conversion: {e}")
        raise


def translate_and_merge_description(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes a DataFrame to add a translated 'description' column,
    append 'Beschreibung' and 'batch_number' to it, and handle exceptions.

    Steps:
    1. Translates the 'description' column and stores it in 'translated_description'.
    2. Appends translated text to 'description' column with a separator '|'.
    3. Merges 'Beschreibung' and 'batch_number' with the updated 'description' column.

    Args:
        df (pd.DataFrame): The input DataFrame with columns 'description', 'Beschreibung', and 'batch_number'.

    Returns:
        pd.DataFrame: The processed DataFrame with an updated 'description' column.
    """

    try:
        # Step 1: Translate the 'description' column
        df["translated_description"] = df["description"].apply(
            lambda x: translate_text(x, tokenizer, model)
        )

        # Step 2: Append translated text to 'description' column with '|'
        df["description"] = df.apply(
            lambda row: f"{row['description']} | {row['translated_description']}",
            axis=1,
        )

        # Step 3: Merge 'Beschreibung' and 'batch_number' with updated 'description' column
        df["description"] = df.apply(
            lambda row: f"{row['description']} | {row['beschreibung']}\n -{row['batch_number']}",
            axis=1,
        )

        # Drop the intermediate 'translated_description' column if not needed
        df.drop(
            columns=["translated_description", "beschreibung", "batch_number"],
            inplace=True,
        )

        logging.info("DataFrame 'description' column processed successfully.")

        return df

    except Exception as e:
        logging.error(f"Error processing DataFrame: {e}")
        return df  # Return DataFrame even if processing fails
    return df


def translate_text(text, tokenizer, model):
    """
    Translates the given text using the specified tokenizer and model.

    Args:
        text (str): The text to translate.
        tokenizer (MarianTokenizer): The tokenizer for the translation model.
        model (MarianMTModel): The translation model.

    Returns:
        str: The translated text.

    Raises:
        Exception: If translation fails, logs the error and returns the original text.
    """
    try:
        tokens = tokenizer([text], return_tensors="pt", padding=True)
        translated = model.generate(**tokens)
        translated_text = tokenizer.batch_decode(translated, skip_special_tokens=True)
        return translated_text[0]
    except Exception as e:
        logging.error(f"Translation error for text '{text}': {e}")
        return text  # Return original text if translation fails
