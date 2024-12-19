import logging
from typing import List, Optional, Tuple

# Configure logging
logger = logging.getLogger("<EMW SLExA ETL>")

# Treatment conversion dictionary
TREATMENT_CONVERSION = {"-U": "UO", "AMO": "AO", "MB": "MBO", "AM": "MAO", "": "AO"}


class TreatmentExtractor:
    def __init__(self, treatment_list: List[str]):
        """
        Initialize the TreatmentExtractor with a list of treatments.

        Args:
            treatment_list (List[str]): The list of valid treatments.
        """
        self.treatment_list = treatment_list

    @staticmethod
    def normalize_string(value: str) -> str:
        """
        Normalize a string by removing hyphens, spaces, and converting to lowercase.

        Args:
            value (str): The string to normalize.

        Returns:
            str: The normalized string.
        """
        return value.replace(" ", "").replace("+", "").lower()

    def extract_treatment(
        self,
        index: int,
        candidate: str,
        best_match: str,
        coating: str,
        source_list: List[str],
    ) -> Optional[str]:
        """
        Extract treatment information by first removing the best match and coating from the candidate,
        then comparing it to the source list.

        Args:
            index (int): The index of the item in the source list.
            candidate (str): The candidate grade string.
            best_match (str): The best match string to be removed.
            coating (str): The coating string to be removed.
            source_list (List[str]): The source list containing treatments.

        Returns:
            Optional[str]: Extracted treatment string or None if no match is found.
        """
        try:
            # Validate index
            if index < 0 or index >= len(source_list):
                raise ValueError(f"Index {index} is out of range for source list.")

            # Normalize input strings
            normalized_candidate = self.normalize_string(candidate)
            normalized_best_match = self.normalize_string(best_match)
            normalized_coating = self.normalize_string(coating) if coating else ""

            # Remove best match and coating from the candidate
            cleaned_candidate = (
                normalized_candidate.replace(normalized_best_match, "")
                .replace(normalized_coating, "")
                .strip()
            )

            # Get the source item for the given index
            source_item = source_list[index]
            treatment = None

            for treat in source_item:
                normalized_source = self.normalize_string(treat)

                # Check for exact match in cleaned_candidate
                if normalized_source in cleaned_candidate:
                    treatment = normalized_source
                    logger.info(
                        f"Treatment '{treatment}' found in candidate '{candidate}'."
                    )
                    break

                # Check for treatment conversion
                for key, val in TREATMENT_CONVERSION.items():
                    if (
                        key.lower() in cleaned_candidate
                        and val.lower() in normalized_source
                    ):
                        treatment = TREATMENT_CONVERSION[key]
                        logger.info(
                            f"Treatment '{key}' converted to '{treatment}' for candidate '{candidate}'."
                        )
                        return treatment.upper()

            if not treatment:
                logger.warning(
                    f"Could not find any treatment in candidate '{candidate}'."
                )
            return treatment.upper() if treatment else None

        except Exception as e:
            logger.error(f"Error extracting treatment for candidate '{candidate}': {e}")
            return None

    @staticmethod
    def longest_common_substring_length(s1: str, s2: str) -> Tuple[int, str]:
        """
        Find the length and value of the longest common substring between two strings.

        Args:
            s1 (str): The first string.
            s2 (str): The second string.

        Returns:
            Tuple[int, str]: Length of the substring and the substring itself.
        """
        try:
            m, n = len(s1), len(s2)
            dp = [[0] * (n + 1) for _ in range(m + 1)]
            max_length = 0
            end_index_s1 = 0

            for i in range(1, m + 1):
                for j in range(1, n + 1):
                    if s1[i - 1] == s2[j - 1]:
                        dp[i][j] = dp[i - 1][j - 1] + 1
                        if dp[i][j] > max_length:
                            max_length = dp[i][j]
                            end_index_s1 = i

            substring = (
                s1[end_index_s1 - max_length : end_index_s1] if max_length > 0 else ""
            )
            return max_length, substring
        except Exception as e:
            logger.error(f"Error computing longest common substring: {e}")
            return 0, ""
