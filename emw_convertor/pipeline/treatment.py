import pandas as pd
import yaml
from typing import Optional, List
import logging
from emw_convertor import global_vars
import numpy as np
from emw_convertor import schema

# Configure logging
logger = logging.getLogger("<Bilstein SLExA ETL>")

treatment_conversion = {"U": "UO", "AMO": "AO"}


class TreatmentExtractor:
    def __init__(self, header_name: str):
        """
        Initialize FinishChecker by loading finishes from a YAML file.

        Args:
            yaml_path (str): Path to the YAML file containing finish data.
        """
        self.header_name = header_name
        self.grade_list = self.get_grades_from_kb(schema, "treatment")

    @staticmethod
    def get_grades_from_kb(schema: List[dict], column: str) -> List[str]:
        """
        Fetch grade names from the database schema.

        Args:
            schema (List[dict]): The schema containing grade information.
            column (str): The column to extract grades from.

        Returns:
            List[str]: A list of grades from the schema.
        """
        try:
            return [item[column] for item in schema if column in item]
        except Exception as e:
            logger.error(f"Error extracting grades from schema: {e}")
            raise ValueError("Failed to fetch grades from schema.") from e

    @staticmethod
    def normalize_string(grade: str) -> str:
        """
        Normalize the grade by removing hyphens, spaces, and converting to lowercase.

        Args:
            grade (str): The grade to normalize.

        Returns:
            str: The normalized grade.
        """
        return grade.replace("-", "").replace(" ", "").replace("+", "").lower()

    def check_and_update_treatment(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
