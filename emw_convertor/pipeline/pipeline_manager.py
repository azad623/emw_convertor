import os
import pandas as pd
from typing import Optional, List, Dict
from emw_convertor import (
    config,
    global_vars,
    log_output_path,
    local_data_input_path,
    schema,
)
from emw_convertor.getters.data_getter import load_excel_file
from emw_convertor.pipeline.transformation import (
    standardize_missing_values,
    drop_rows_with_missing_values,
)

# from emw_convertor.pipeline.schema_validation import identify_header_name
from emw_convertor.pipeline.extractor import ExtractorRunner
from emw_convertor.pipeline.grade_coating_extractor import GradeCoatingExtractor
from emw_convertor.pipeline.dimension_extractor import DimensionExtractor
from emw_convertor.pipeline.treatment import TreatmentExtractor
from emw_convertor.utils.helper import (
    delete_all_files,
    load_schema_list,
    validate_output,
)
from emw_convertor.config.logging_system import setup_logger


def pipeline_run(header_names: Dict, file_path: str) -> pd.DataFrame:
    """
    Orchestrates the ETL pipeline, managing each step sequentially.

    Args:
        header_name (str): The name of the header column to process.
        file_path (str): Path to the Excel file to be processed.

    Returns:
        pd.DataFrame: The processed DataFrame.

    Raises:
        ValueError: If the file cannot be processed.
    """
    global global_vars

    # Ensure global_vars is initialized as a dictionary
    if not isinstance(global_vars, dict):
        global_vars = {"error_list": []}
    else:
        global_vars["error_list"] = []  # Reset the error list

    delete_all_files(os.path.join(local_data_input_path, "interim"))
    # delete_all_files(log_output_path)

    file_name = os.path.basename(file_path)
    logger = setup_logger(file_name, config)
    logger.info(f"Starting processing for file: {file_path}")

    try:
        # Step 1: Load the Excel file
        logger.info("<< Step 1: Loading Excel file from pre-defined location >>")
        df = load_excel_file(file_path)
        if df is None:
            raise ValueError(
                f"Loader failed to load Excel file into a DataFrame for: {file_path}"
            )

        # Step 2: Drop rows with missing values above the threshold
        logger.info("<< Step 2: Dropping rows with excessive missing values >>")
        df = drop_rows_with_missing_values(df, threshold=0.7)

        # Step 3: Standardize missing values
        logger.info("<< Step 3: Standardizing missing values in the DataFrame >>")
        standardize_missing_values(df)

        # Step 4: Check and update the grade column
        logger.info("<< Step 4: Validating and updating the grade column >>")
        grade_coating_extractor = GradeCoatingExtractor(
            grade_coating_list=load_schema_list(schema, "grade_coating"),
            grade_list=load_schema_list(schema, "base_grade"),
            coating_list=load_schema_list(schema, "coating"),
        )

        dimension_extractor = DimensionExtractor(column_name=header_names["dimensions"])

        treatment_extractor = TreatmentExtractor(
            treatment_list=load_schema_list(schema, "treatment")
        )

        extractor_runner = ExtractorRunner(
            header_names=header_names,
            grade_coating_extractor=grade_coating_extractor,
            dimension_extractor=dimension_extractor,
            treatment_extractor=treatment_extractor,
        )

        df = extractor_runner.run_extractor(df)

        del (
            grade_coating_extractor,
            extractor_runner,
            treatment_extractor,
        )  # Free up resources

        # Step 6: Log success
        logger.info(f"File {file_path} processed successfully.")
        # global_vars["error_list"].append(f"File {file_path} processed successfully.")

        # df.to_excel("demo.xlsx", sheet_name="sheet1", index=False)

        status, global_vars = validate_output(df)

        return status, df, global_vars

    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        global_vars["error_list"].append(str(ve))
        raise

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        global_vars["error_list"].append(f"Unexpected error: {e}")
        raise RuntimeError(f"Pipeline failed for {file_path}") from e


if __name__ == "__main__":
    # Example usage
    try:
        # This should be dynamically identified or passed
        header_names = {"grades": "Materialkurztext", "dimensions": "Materialkurztext"}

        file_path = "/Users/azadabad/Downloads/emw/Anfrage + Kalkulationstabellen/Anfrage Kunde 10000+.xlsx"
        processed_df = pipeline_run(header_names, file_path)

        # Save or use the processed DataFrame as needed
        processed_df.to_excel("/path/to/processed/file.xlsx", index=False)
        print("Pipeline executed successfully.")
    except Exception as ex:
        print(f"Pipeline execution failed: {ex}")
