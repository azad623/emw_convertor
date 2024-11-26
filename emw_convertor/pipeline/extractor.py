import pandas as pd
import logging
from typing import List, Tuple, Optional, Dict
from emw_convertor import global_vars, schema
from emw_convertor.pipeline.grade_coating_extractor import GradeCoatingExtractor
from emw_convertor.pipeline.dimension_extractor import DimensionExtractor
from emw_convertor.pipeline.treatment import TreatmentExtractor

# Configure logging
logger = logging.getLogger("<EMW SLExA ETL>")


class ExtractorRunner:
    def __init__(
        self,
        header_names: Dict,
        grade_coating_extractor: GradeCoatingExtractor,
        dimension_extractor: DimensionExtractor,
        treatment_extractor: TreatmentExtractor,
    ):
        """
        Initialize the ExtractorRunner.

        Args:
            header_names (Dict): Mapping of column headers for grades, dimensions, etc.
            grade_coating_extractor (GradeCoatingExtractor): An instance of GradeCoatingExtractor.
        """
        self.header_names = header_names
        self.grade_coating_extractor = grade_coating_extractor
        self.dimension_extractor = dimension_extractor
        self.treatment_extractor = treatment_extractor

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
            if header not in df.columns:
                logger.error(f"Column '{header}' not found in DataFrame.")
                raise ValueError(f"Column '{header}' not found in DataFrame.")

        df["Güte"] = None  # Grade
        df["Auflage"] = None  # Coating
        df["Oberfläche"] = None

        for idx, candidate in df[self.header_names["grades"]].items():
            if not isinstance(candidate, str) or not candidate.strip():
                logger.warning(f"Row {idx}: Candidate is empty or invalid.")
                continue

            # Try matching with grade_coating_list first
            index, best_match, matched, score = (
                self.grade_coating_extractor.find_best_match(
                    candidate, self.grade_coating_extractor.grade_coating_list, 0.6
                )
            )
            if matched:
                coating = self.grade_coating_extractor.extract_coating_code(
                    index,
                    candidate,
                    best_match,
                    self.grade_coating_extractor.grade_coating_list,
                    self.grade_coating_extractor.coating_list,
                )
                treatment = self.treatment_extractor.extract_treatment(
                    index,
                    candidate,
                    best_match,
                    coating,
                    self.treatment_extractor.treatment_list,
                )

                logger.info(
                    f"Candidate '{candidate}' matched with grade & coating '{best_match}' "
                    f"and coating '{coating}' (Score: {score:.2f})."
                )
                df.at[idx, "Güte"] = best_match
                df.at[idx, "Auflage"] = coating
                df.at[idx, "Oberfläche"] = treatment
            else:
                # Fallback to grade_list if no match found in grade_coating_list
                index, best_match, matched, score = (
                    self.grade_coating_extractor.find_best_match(
                        candidate, self.grade_coating_extractor.grade_list, 0.6
                    )
                )
                if matched:
                    logger.info(
                        f"Candidate '{candidate}' matched with grade '{best_match}' and no coating "
                        f"(Score: {score:.2f})."
                    )

                    treatment = self.treatment_extractor.extract_treatment(
                        index,
                        candidate,
                        best_match,
                        coating,
                        self.treatment_extractor.treatment_list,
                    )

                    df.at[idx, "Güte"] = best_match
                    df.at[idx, "Auflage"] = None
                    df.at[idx, "Oberfläche"] = treatment
                else:
                    logger.warning(
                        f"Candidate '{candidate}' at index {idx} did not match any reference."
                    )
                    df.at[idx, "Güte"] = None
                    df.at[idx, "Auflage"] = None
                    df.at[idx, "Oberfläche"] = None
        print(df.columns)
        # Extract dimensions if applicable
        if (
            "dimensions" in self.header_names
            and self.header_names["dimensions"] is not None  # Fixed the typo here
        ):
            df = self.dimension_extractor.extract_dimensions(df)

        return df
