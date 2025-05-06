import pandas as pd
import logging
from typing import List, Tuple, Optional, Dict
from emw_convertor import global_vars, grades_schema, coating_schema
from emw_convertor.pipeline.grade_extractor import GradeExtractor
from emw_convertor.pipeline.dimension_extractor import DimensionExtractor
from emw_convertor.pipeline.coating_treatment import CoatingTreatmentExtractor

# Configure logging
logger = logging.getLogger("<EMW SLExA ETL>")


class ExtractorRunner:
    def __init__(
        self,
        header_names: Dict,
        grade_extractor: GradeExtractor,
        dimension_extractor: DimensionExtractor,
        coating_treatment_extractor: CoatingTreatmentExtractor,
    ):
        """
        Initialize the ExtractorRunner.

        Args:
            header_names (Dict): Mapping of column headers for grades, dimensions, etc.
            grade_extractor (GradeCoatingExtractor): An instance of GradeCoatingExtractor.
        """
        self.header_names = header_names
        self.grade_extractor = grade_extractor
        self.dimension_extractor = dimension_extractor
        self.coating_treatment_extractor = coating_treatment_extractor

    def normalize_string(self, value: str) -> str:
        """
        Normalize the string by removing spaces, hyphens, plus signs, and converting to lowercase.

        Args:
            value (str): The string to normalize.

        Returns:
            str: The normalized string.
        """
        return value.replace(" ", "").replace("-", "").replace("+", "").lower()

    def run_extractor(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run the extraction process for grades, coatings, and dimensions.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The updated DataFrame with extracted fields.
        """
        # Validate column existence
        for header in self.header_names.values():
            if (header not in df.columns) and (header not in "None"):
                logger.error(f"Column '{header}' not found in DataFrame.")
                raise ValueError(f"Column '{header}' not found in DataFrame.")

        if self.header_names["grades"] is not "None":
            df["Güte_"] = None  # Grade
            df["Auflage_"] = None  # Coating
            df["Oberfläche_"] = None

            for idx, candidate in df[self.header_names["grades"]].items():
                if not isinstance(candidate, str) or not candidate.strip():
                    logger.warning(f"Row {idx}: Candidate is empty or invalid.")
                    continue

                if idx == 172:
                    logger.info(f"Row {idx}: Candidate is {candidate}")

                # Try matching with grade_coating_list first
                best_match, matched = self.grade_extractor.extract_grade(candidate, 0.2)
                if matched:
                    normalized_candidate = self.normalize_string(candidate)
                    normalized_best_grade = self.normalize_string(best_match)

                    # Remove best grade from the candidate to get potential string
                    # Find the position of normalized_best_grade in the string
                    # Get index of normalized_best_grade in the string
                    # Find the position of normalized_best_grade in the string
                    index = normalized_candidate.find(normalized_best_grade)
                    if index != -1:
                        # Extract everything before the match
                        before_match = normalized_candidate[:index]
                        # Remove alphabetic characters except 'x' from the part before the match
                        filtered_before = "".join(
                            c
                            for c in before_match
                            if c == "x" or not c.isalpha() or c.isdigit()
                        )

                        # Get everything after the best grade
                        after_match = normalized_candidate[
                            index + len(normalized_best_grade) :
                        ]

                        # Combine filtered before part with after part (skipping the best grade itself)
                        candidate = filtered_before + after_match
                    else:
                        # If no match found, apply the filtering to the entire string
                        candidate = "".join(
                            c
                            for c in normalized_candidate
                            if c == "x" or not c.isalpha() or c.isdigit()
                        )

                    grade, coating, treatment = (
                        self.coating_treatment_extractor.extract_coating_treatment(
                            candidate, best_match
                        )
                    )

                    df.at[idx, "Güte_"] = grade
                    df.at[idx, "Auflage_"] = coating
                    df.at[idx, "Oberfläche_"] = treatment

                else:
                    candidate = self.normalize_string(candidate)

        else:
            logger.error(
                "Die Spalten „Güt“, „Auflage“ und „Oberfläche“ werden nicht aktualisiert. Bitte wählen Sie den korrekten Spaltennamen"
            )
            global_vars["error_list"].append(
                "Die Spalten „Güt“, „Auflage“ und „Oberfläche“ werden nicht aktualisiert. Bitte wählen Sie den korrekten Spaltennamen"
            )
        # Extract dimensions if applicable
        if (
            "dimensions" in self.header_names
            and self.header_names["dimensions"] is not "None"  # Fixed the typo here
        ):
            df = self.dimension_extractor.extract_dimensions(df)
        else:
            logger.error(
                "Die Spalten Dimension werden nicht aktualisiert. Bitte wählen Sie den korrekten Spaltennamen"
            )
            global_vars["error_list"].append(
                "Die Spalten Dimension werden nicht aktualisiert. Bitte wählen Sie den korrekten Spaltennamen"
            )

        return df
