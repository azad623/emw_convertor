import logging
from typing import List, Optional, Tuple

logger = logging.getLogger("GradeExtractor")


class GradeExtractor:
    """
    A class to extract grade information from material descriptions by finding the
    best match against a reference grade list after normalization.
    """

    def __init__(self, grade_list: List[str]):
        """
        Initialize the GradeExtractor with a list of reference grades.

        Args:
            grade_list (List[str]): A list of standard grades to match against.
        """
        self.grade_list = grade_list
        # Pre-normalize the grade list for efficiency
        self.normalized_grades = [self.normalize_string(grade) for grade in grade_list]

    @staticmethod
    def normalize_string(value: str) -> str:
        """
        Normalize the string by removing spaces, hyphens, plus signs, and converting to lowercase.

        Args:
            value (str): The string to normalize.

        Returns:
            str: The normalized string.
        """
        return value.replace(" ", "").replace("-", "").replace("+", "").lower()

    @staticmethod
    def longest_common_substring(s1: str, s2: str) -> Tuple[int, str]:
        """
        Find the length and value of the longest common substring between two strings.

        Args:
            s1 (str): The first string.
            s2 (str): The second string.

        Returns:
            Tuple[int, str]: Length of the substring and the substring itself.
        """
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

    def extract_grade(
        self, material_description: str, threshold: float = 0.5
    ) -> Optional[str]:
        """
        Extract the grade from a material description by finding the best match
        from the reference grade list after normalization.

        Args:
            material_description (str): The material description to process.
            threshold (float): Minimum match length to consider a match valid.

        Returns:
            Optional[str]: The extracted grade or None if no match found.
        """
        try:
            # First normalize the input material description

            separators = [" ", "-", ";", "+", "x", "X"]
            words = []

            # Start with the full string
            current_text = material_description

            # Process for each separator
            for separator in separators:
                new_words = []
                for text in current_text.split(separator):
                    text = text.strip()
                    if text:  # Skip empty strings
                        new_words.append(text)

                # Update the list of words and current text pieces
                words.extend(new_words)
                current_text = " ".join(new_words)

            # Add the original description as well
            words.append(material_description)

            # Remove duplicates while preserving order
            seen = set()
            words = [word for word in words if not (word in seen or seen.add(word))]

            # Step 2: Try exact matching (case-insensitive)
            for word in words:
                for i, grade in enumerate(self.grade_list):
                    # Try exact match (case-insensitive)
                    if word.lower() == grade.lower():
                        return grade, True

            best_grade = None
            best_match_length = 0
            matched = False

            normalized_input = self.normalize_string(material_description)

            # Look for the longest normalized grade that appears in the normalized input
            for i, normalized_grade in enumerate(self.normalized_grades):
                if normalized_grade in normalized_input:
                    # Direct match found - use the grade with longest match
                    if len(normalized_grade) > best_match_length:
                        best_match_length = len(normalized_grade)
                        best_grade = self.grade_list[i]

            # If we found a direct match, return it
            if best_grade:
                return best_grade, True

            # If no direct match, use the longest common substring approach
            """
            for i, normalized_grade in enumerate(self.normalized_grades):
                match_length, match_str = self.longest_common_substring(
                    normalized_input, normalized_grade
                )

                # Consider match only if it has a significant length relative to the grade
                match_ratio = match_length / len(normalized_grade)

                if match_length > best_match_length and match_ratio >= threshold:
                    best_match_length = match_length
                    best_grade = self.grade_list[i]
                    matched = True
            """

            return best_grade, matched

        except Exception as e:
            logger.error(f"Error extracting grade: {e}")
            return None
