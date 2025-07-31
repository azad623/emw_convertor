import logging
import re
from typing import List, Optional, Tuple

# Configure logging
logger = logging.getLogger("<EMW SLExA ETL>")

# Treatment conversion dictionary
TREATMENT_CONVERSION = {
    "A": "AO",
    "U": "UO",
    "MB": "MBO",
    "MC": "MCO",
    "NA": "NAC",
    "B": "BO",
    "MA": "MAC",
    "E": "EO",
    "NAC": "MAC",
    "AM": "AMO",
}


class CoatingTreatmentExtractor:
    def __init__(self, treatment_dict: dict):
        """
        Initialize the CoatingTreatmentExtractor with a treatment dictionary.

        Args:
            treatment_dict (dict): Dictionary containing symbol, prefix_coating, coating, and treatment.
        """
        self.treatment_list = treatment_dict

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

    def generate_coating_permutations(self, prefix, coatings) -> List[str]:
        """
        Generate all permutations of prefix_coating and coating items.

        Returns:
            List[str]: List of all possible coating combinations.
        """
        permutations = []
        if prefix and coatings:
            for coating in coatings:
                permutations.append(f"{prefix}{coating}")
        elif coatings:
            permutations.extend(coatings)
        elif prefix:
            permutations.append(prefix)

        return permutations

    def get_treatment(self, potential_str, treatments):
        """
        Extract treatment from potential string and return both the treatment and remaining string.

        Args:
            potential_str (str): String to search for treatment
            treatments (list): List of possible treatments

        Returns:
            tuple: (matched_treatment, remaining_string)
        """
        best_match_length = 0
        matched_treatment = None
        matched_treatment_normalized = None
        remaining_str = potential_str

        # First check direct matches
        for treatment in treatments:
            normalized_treatment = self.normalize_string(treatment)
            if normalized_treatment in potential_str:
                if len(normalized_treatment) > best_match_length:
                    best_match_length = len(normalized_treatment)
                    matched_treatment = treatment
                    matched_treatment_normalized = normalized_treatment

        if matched_treatment:
            # Remove the matched treatment from the string
            remaining_str = potential_str.replace(
                matched_treatment_normalized, " "
            ).strip()
            for key, val in TREATMENT_CONVERSION.items():
                normalized_key = key.lower()
                if normalized_key == matched_treatment.lower():
                    matched_treatment = val
                    logger.info(
                        f"Treatment '{key}' converted to '{matched_treatment}' for candidate '{potential_str}'."
                    )
                    break
        else:
            # If no direct match, check for partial matches
            for treatment in TREATMENT_CONVERSION.keys():
                normalized_treatment = self.normalize_string(treatment)
                for split_segment in potential_str.split(" "):
                    if normalized_treatment == split_segment.strip():
                        if len(normalized_treatment) > best_match_length:
                            best_match_length = len(normalized_treatment)
                            matched_treatment = treatment
                            matched_treatment_normalized = normalized_treatment

            if matched_treatment:
                # Remove the matched treatment from the string
                remaining_str = potential_str.replace(
                    matched_treatment_normalized, " "
                ).strip()
                for key, val in TREATMENT_CONVERSION.items():
                    normalized_key = key.lower()
                    if normalized_key == matched_treatment.lower():
                        matched_treatment = val
                        logger.info(
                            f"Treatment '{key}' converted to '{matched_treatment}' for candidate '{potential_str}'."
                        )
                        break

        return matched_treatment, remaining_str

    def _preprocess_coating_string(self, potential_str: str) -> str:
        """
        Preprocess the coating string to handle common pattern variations.

        This method fixes issues where coating patterns like GI/50/50 should match
        GI50/50 patterns in the schema.

        Args:
            potential_str (str): The original string to preprocess

        Returns:
            str: The preprocessed string with pattern variations handled
        """

        # Handle cases like GI/50/50 -> GI50/50
        # Pattern: prefix followed by slash followed by coating pattern
        # Common prefixes: GI, EG, AS, ZE, etc.
        coating_prefixes = ["GI", "EG", "AS", "ZE", "Z", "G"]

        processed_str = potential_str

        for prefix in coating_prefixes:
            # Pattern: prefix/coating (e.g., GI/50/50 -> GI50/50)
            pattern = f"{prefix}/"
            if pattern.lower() in processed_str.lower():
                # Replace the slash after the prefix
                processed_str = re.sub(
                    f"({prefix})/", r"\1", processed_str, flags=re.IGNORECASE
                )

        return processed_str

    def extract_coating_treatment(
        self,
        potential_str: str,
        best_grade: str,
    ) -> Tuple[str, Optional[str], Optional[str], str]:
        """
        Extract treatment and coating information from a candidate string.

        Args:
            candidate (str): The candidate grade string.
            best_grade (str): The best match grade string to be removed.

        Returns:
            Tuple[str, Optional[str], Optional[str], str]:
                - best_grade+symbol+prefix_coating
                - coating
                - treatment
                - remaining_unmatched_string
        """
        try:
            # Keep track of the original string for remaining calculation
            original_potential_str = potential_str

            # Preprocess the string to handle common coating pattern variations
            # Fix issue where GI/50/50 should match GI50/50 pattern
            preprocessed_str = self._preprocess_coating_string(potential_str)

            # Try to match coating permutations in potential_str
            matched_coating = None
            symbol = ""
            prefix_coating = ""
            matched_treatment = None
            remaining_after_coating = preprocessed_str

            for index, coating_ in enumerate(self.treatment_list):
                # generate coating permutations list
                potential_coating_permutations = self.generate_coating_permutations(
                    coating_["prefix_coating"], coating_["coating"]
                )
                for index, coating_permutation in enumerate(
                    potential_coating_permutations
                ):
                    normalized_coating = self.normalize_string(coating_permutation)
                    if normalized_coating in (preprocessed_str):
                        matched_coating = coating_["coating"][index]
                        # Remove the matched coating from preprocessed_str
                        remaining_after_coating = preprocessed_str.replace(
                            normalized_coating, " "
                        ).strip()

                        symbol = coating_["symbol"]
                        prefix_coating = coating_["prefix_coating"]

                        matched_treatment, remaining_after_treatment = (
                            self.get_treatment(
                                remaining_after_coating, coating_["treatment"]
                            )
                        )
                        remaining_after_coating = remaining_after_treatment

            if best_grade is not None:
                # Construct the output string for the first return value
                grade_with_prefix = best_grade + symbol + prefix_coating
            else:
                grade_with_prefix = None

            # Log the results
            if matched_coating:
                logger.info(
                    f"Coating '{matched_coating}' found in candidate '{original_potential_str}'."
                )
            else:
                logger.warning(
                    f"Could not find any coating in candidate '{original_potential_str}'."
                )
                try:
                    treatments = self.treatment_list[-1]["treatment"]
                    matched_treatment, remaining_after_coating = self.get_treatment(
                        preprocessed_str, treatments
                    )
                except Exception as e:
                    logger.error(
                        f"Error extracting coating for candidate '{original_potential_str}': {e}"
                    )

            if matched_treatment:
                logger.info(
                    f"Treatment '{matched_treatment}' found in candidate '{original_potential_str}'."
                )
            else:
                logger.warning(
                    f"Could not find any treatment in candidate '{original_potential_str}'."
                )

            # Clean up the remaining string - remove extra spaces and common non-meaningful parts
            final_remaining = remaining_after_coating.strip()
            # Remove common dimension patterns that might be left over
            final_remaining = re.sub(
                r"\d+[.,]?\d*\s*[xX*]\s*\d+[.,]?\d*(?:\s*[xX*]\s*\d+[.,]?\d*)?",
                "",
                final_remaining,
            )
            final_remaining = re.sub(r"\s+", " ", final_remaining).strip()

            return (
                grade_with_prefix,
                matched_coating,
                matched_treatment,
                final_remaining,
            )

        except Exception as e:
            logger.error(
                f"Error extracting treatment for candidate '{potential_str}': {e}"
            )
            return best_grade, None, None, potential_str

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


# Example usage:
if __name__ == "__main__":
    # Sample treatment dictionary
    treatment_dict = {
        "symbol": "+",
        "prefix_coating": "Z",
        "coating": ["07/07", "10/10"],
        "treatment": ["UO", "EO", "MAO", "MAC", "MBO", "MBC"],
    }

    extractor = CoatingTreatmentExtractor(treatment_dict)

    # Example candidates
    candidate1 = "S350GD+Z10/10-UO"
    candidate2 = "S280GD+Z07/07MAO"

    # Extract treatment for the candidates
    result1 = extractor.extract_coating_treatment(candidate1, "S350GD")
    result2 = extractor.extract_coating_treatment(candidate2, "S280GD")

    print(f"Candidate: {candidate1}")
    print(f"Result: {result1}")
    print(
        f"Grade: {result1[0]}, Coating: {result1[1]}, Treatment: {result1[2]}, Remaining: '{result1[3]}'"
    )
    print()
    print(f"Candidate: {candidate2}")
    print(f"Result: {result2}")
    print(
        f"Grade: {result2[0]}, Coating: {result2[1]}, Treatment: {result2[2]}, Remaining: '{result2[3]}'"
    )
