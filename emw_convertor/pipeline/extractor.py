import logging
from typing import Dict

import pandas as pd

from emw_convertor import global_vars
from emw_convertor.pipeline.coating_treatment import CoatingTreatmentExtractor
from emw_convertor.pipeline.dimension_extractor import DimensionExtractor
from emw_convertor.pipeline.grade_extractor import GradeExtractor

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

    def _remove_dimension_patterns(self, text: str) -> str:
        """
        Remove dimension patterns from text to avoid false highlighting.

        Removes patterns like:
        - 3,0x271 (thickness x width)
        - 1,5x1056x419 (thickness x width x length)
        - Standalone numbers and 'x' characters from dimensions

        Args:
            text (str): The text to clean

        Returns:
            str: Text with dimension patterns removed
        """
        import re

        if not text or not text.strip():
            return text

        # Remove dimension patterns like "3,0x271", "1,5x1056x419", etc.
        # Pattern: number,number x number (x number optional)
        dimension_pattern = r"\b\d+[,\.]\d+x\d+(?:x\d+)?\b"
        text = re.sub(dimension_pattern, "", text, flags=re.IGNORECASE)

        # Remove standalone dimension numbers (like "3,0", "271", "419")
        standalone_numbers = r"\b\d+[,\.]\d+\b|\b\d{2,4}\b"
        text = re.sub(standalone_numbers, "", text)

        # Remove standalone 'x' characters that are dimension separators
        text = re.sub(r"\bx\b", "", text, flags=re.IGNORECASE)

        # Clean up extra spaces
        text = re.sub(r"\s+", " ", text).strip()

        return text

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

        # Always add highlighting columns for consistency
        df["Unmatched_Remainder_"] = None  # Track unmatched strings
        df["Highlight_Row_"] = (
            False  # Flag for yellow highlighting (unmatched remainders)
        )
        df["Red_Highlight_Row_"] = False  # Flag for red highlighting (no grade found)

        if self.header_names["grades"] != "None":
            df["Güte_"] = None  # Grade
            df["Auflage_"] = None  # Coating
            df["Oberfläche_"] = None  # Treatment

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
                        candidate = filtered_before + " " + after_match
                    else:
                        # If no match found, apply the filtering to the entire string
                        candidate = "".join(
                            c
                            for c in normalized_candidate
                            if c == "x" or not c.isalpha() or c.isdigit()
                        )

                    grade, coating, treatment, remaining_unmatched = (
                        self.coating_treatment_extractor.extract_coating_treatment(
                            candidate, best_match
                        )
                    )

                    # Clean dimension patterns from unmatched remainder
                    cleaned_remainder = self._remove_dimension_patterns(
                        remaining_unmatched
                    )

                    df.at[idx, "Güte_"] = grade
                    df.at[idx, "Auflage_"] = coating
                    df.at[idx, "Oberfläche_"] = treatment
                    df.at[idx, "Unmatched_Remainder_"] = cleaned_remainder

                    # Check if there's significant unmatched content that should be highlighted
                    if cleaned_remainder and len(cleaned_remainder.strip()) > 0:
                        # Filter out common meaningless remainders
                        cleaned_remainder_lower = cleaned_remainder.strip().lower()

                        # Define meaningful patterns (including short ones)
                        meaningful_patterns = [
                            "bondal",
                            "hsa",
                            "dh",
                            "cr",
                            "dc",
                            "bg",
                            "phsultraform",
                            "ultraform",
                            "ymagine",
                            "scalur",
                            "ungehärtet",
                        ]

                        # Check if remainder is meaningful
                        is_meaningful = (
                            any(
                                pattern in cleaned_remainder_lower
                                for pattern in meaningful_patterns
                            )
                            or len(cleaned_remainder_lower)
                            > 5  # Long remainders are usually meaningful
                            or (
                                len(cleaned_remainder_lower) >= 2
                                and cleaned_remainder_lower.isalpha()
                            )  # Short alphabetic remainders
                        )

                        if is_meaningful:
                            df.at[idx, "Highlight_Row_"] = True
                            logger.info(
                                f"Row {idx}: Unmatched remainder '{cleaned_remainder}' - flagged for highlighting"
                            )

                else:
                    # No grade found - flag for red highlighting
                    df.at[idx, "Red_Highlight_Row_"] = True
                    logger.info(
                        f"Row {idx}: No grade found - flagged for red highlighting"
                    )
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
            and self.header_names["dimensions"] != "None"  # Fixed the typo here
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
