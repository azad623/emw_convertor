import pandas as pd
import logging
from typing import List, Tuple, Optional, Dict
from emw_convertor import global_vars, schema
from emw_convertor.pipeline.dimention_extractor import extract_dimensions

# Configure logging
logger = logging.getLogger("<Bilstein SLExA ETL>")


class Extractor:
    def __init__(self, header_names: Dict):
        """
        Initialize the GradeCoatingExtractor with a column header name and a match threshold.

        Args:
            header_name (str): The column header containing grades to check.
            threshold (float): Minimum match score to accept a match. Default is 0.6.
        """
        self.header_names = header_names
        self.grade_list = self.get_grades_from_kb(schema, "base_grade")
        self.grade_coating_list = self.get_grades_from_kb(schema, "grade_coating")
        self.coating_list = self.get_grades_from_kb(schema, "coating")

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

    def find_best_match(
        self, candidate: str, ref_list: List[str], threshold
    ) -> Tuple[Optional[str], bool, float]:
        """
        Find the best match for a candidate grade from the reference list.

        Args:
            candidate (str): The candidate grade to match.
            ref_list (List[str]): The reference list to match against.

        Returns:
            Tuple[Optional[str], bool, float]: The best matching grade, match status, and score.
        """
        normalized_candidate = self.normalize_string(candidate)
        best_match = None
        highest_score = 0
        best_index = 0

        for index, reference in enumerate(ref_list):
            normalized_reference = self.normalize_string(reference)

            # Skip if reference is not in the candidate
            if normalized_reference not in normalized_candidate:
                continue

            # Calculate the match score
            match_score = self.calculate_match_score(
                normalized_candidate, normalized_reference
            )

            if match_score > highest_score:
                highest_score = match_score
                best_match = reference
                best_index = index

        return (best_index, best_match, highest_score >= threshold, highest_score)

    @staticmethod
    def calculate_match_score(candidate: str, reference: str) -> float:
        """
        Calculate the match score between a candidate and a reference grade.

        Args:
            candidate (str): The normalized candidate grade.
            reference (str): The normalized reference grade.

        Returns:
            float: The match score based on substring length and penalties.
        """
        common_length_count, _ = Extractor.longest_common_substring_length(
            candidate, reference
        )

        # Score is proportional to the length of the common substring
        base_score = common_length_count / len(reference) if len(reference) > 0 else 0

        # Penalize if the match is too small compared to the candidate
        if common_length_count < len(candidate) * 0.2:
            base_score *= 0.5  # Reduce score by 50%

        return base_score

    @staticmethod
    def longest_common_substring_length(s1: str, s2: str) -> int:
        """
        Find the length of the longest common substring between two strings.

        Args:
            s1 (str): The first string.
            s2 (str): The second string.

        Returns:
            int: The length of the longest common substring.
        """
        m, n = len(s1), len(s2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        max_length = 0
        end_index_s1 = 0

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if s1[i - 1] == s2[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1] + 1
                    max_length = max(max_length, dp[i][j])
                    end_index_s1 = i

        return max_length, (
            s1[end_index_s1 - max_length : end_index_s1] if max_length > 0 else ""
        )

    def extract_coating_code(
        self,
        index: int,
        candidate: str,
        coating: str,
        grade_list: List[str],
        coating_list: List[str],
    ) -> Optional[str]:
        """
        Extract the coating code by removing the shared substring between the coating and the grade.

        Args:
            index (int): The index of the coating in the list.
            coating (str): The coating string to process.
            grade_list (List[str]): The list of grade strings.

        Returns:
            Optional[str]: The coating code after removing the shared substring,
                        or None if the index is out of range or no shared substring exists.
        """
        try:
            # Validate index
            if index < 0 or index >= len(grade_list):
                raise ValueError(f"Index {index} is out of range for grade list.")

            # Get the grade from the list using the provided index
            grade = grade_list[index]
            coating_list = coating_list[index]

            # Normalize both coating and grade strings
            normalized_coating = Extractor.normalize_string(coating)
            normalized_grade = Extractor.normalize_string(grade)
            normalized_candidate = Extractor.normalize_string(candidate)

            # Find the best match for candidate (from coating)
            best_index, best_match, matched, highest_score = self.find_best_match(
                candidate=normalized_candidate, ref_list=coating_list, threshold=0.5
            )

            if matched:
                return best_match
            else:

                # Find the longest shared substring ( From just )
                _, shared_value = Extractor.longest_common_substring_length(
                    normalized_coating, normalized_grade
                )

                if not shared_value:
                    # No shared substring found, return the original coating
                    return coating

                # Remove the shared substring from the coating
                coating_code = normalized_coating.replace(shared_value, "").strip()

                return coating_code.upper() if coating_code else None

        except Exception as e:
            # Log or raise the error
            logger.error(f"Error extracting coating code: {e}")
            return None

    def run_extractor(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check and update grades in a DataFrame based on the database reference.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The updated DataFrame with validated grades.
        """
        for _, vals in self.header_names.items():
            if vals not in df.columns:
                logger.error(f"Column '{self.header_names}' not found in DataFrame.")
                raise ValueError(
                    f"Column '{self.header_names}' not found in DataFrame."
                )

        df["Güte"] = None  # Initialize the 'grade' column
        df["Auflage"] = None
        df["Oberfläche"] = None

        msterial_info = {"grade": None, "grad_coating": None, "coating": None}

        for idx, candidate in df[self.header_names["grades"]].items():
            if not isinstance(candidate, str) or not candidate.strip():
                message = f"Line item at index {idx} is empty or invalid."
                global_vars["error_list"].append(message)
                logger.warning(message)
                continue

            # Try matching with grade_coating_list first
            index, best_match, matched, score = self.find_best_match(
                candidate, self.grade_coating_list, 0.6
            )
            if matched:
                coating = self.extract_coating_code(
                    index, candidate, best_match, self.grade_list, self.coating_list
                )
                logger.info(
                    f"Candidate '{candidate}' matched with grade & coating '{best_match}' and coating {coating} (Score: {score:.2f})."
                )

                msterial_info["grad_coating"] = best_match
                msterial_info["coating"] = coating

            else:
                # Fallback to grade_list if no match found in grade_coating_list
                index, best_match, matched, score = self.find_best_match(
                    candidate, self.grade_list, 0.6
                )
                if matched:
                    logger.info(
                        f"Candidate '{candidate}' matched with grade & coating '{best_match}' and coating None (Score: {score:.2f})."
                    )
                    msterial_info["grad_coating"] = best_match
                    msterial_info["coating"] = None

                else:
                    message = f"Candidate '{candidate}' at index {idx} did not match any reference."
                    msterial_info["grad_coating"] = None
                    msterial_info["coating"] = None
                    global_vars["error_list"].append(message)
                    logger.warning(message)

            # Update DataFrame with best match or None
            df.at[idx, "Güte"] = msterial_info["grad_coating"]
            df.at[idx, "Auflage"] = msterial_info["coating"]

        # Parse and exctrct dimensions
        if self.header_names["dimensions"]:
            df = extract_dimensions(df, self.header_names["dimensions"])

        return df
