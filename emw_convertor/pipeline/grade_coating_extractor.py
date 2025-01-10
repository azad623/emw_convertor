import logging
from typing import List, Tuple, Optional

logger = logging.getLogger("<EMW SLExA ETL>")


class GradeCoatingExtractor:
    """
    A class to extract grade and coating information from material descriptions.
    """

    def __init__(
        self,
        grade_coating_list: List[str],
        grade_list: List[str],
        coating_list: List[str],
    ):
        """
        Initialize the GradeCoatingExtractor.

        Args:
            grade_coating_list (List[str]): A list of grade-coating combinations from the schema.
            grade_list (List[str]): A list of grades from the schema.
            coating_list (List[str]): A list of coatings from the schema.
        """
        self.grade_coating_list = grade_coating_list
        self.grade_list = grade_list
        self.coating_list = coating_list

    @staticmethod
    def normalize_string(value: str) -> str:
        """
        Normalize the string by removing spaces, hyphens, and converting to lowercase.

        Args:
            value (str): The string to normalize.

        Returns:
            str: The normalized string.
        """
        return value.replace(" ", "").replace("-", "").replace("+", "").lower()

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
            normalized_coating = GradeCoatingExtractor.normalize_string(coating)
            normalized_grade = GradeCoatingExtractor.normalize_string(grade)
            normalized_candidate = GradeCoatingExtractor.normalize_string(candidate)

            # Find the best match for candidate (from coating)
            best_index, best_match, matched, highest_score = self.find_best_match(
                candidate=normalized_candidate,
                reference_list=coating_list,
                threshold=0.5,
            )

            if matched:
                return best_match
            else:

                # Find the longest shared substring ( From just )
                _, shared_value = GradeCoatingExtractor.longest_common_substring_length(
                    normalized_coating, normalized_grade
                )

                if not shared_value:
                    # No shared substring found, return the original coating
                    return coating

                # Remove the shared substring from the coating
                coating_code = normalized_coating.replace(shared_value, "").strip()

                return coating_code.upper() if coating_code.strip() else None

        except Exception as e:
            # Log or raise the error
            logger.error(f"Error extracting coating code: {e}")
            return None

        """
        Calculate the match score between a candidate and a reference grade.

        Args:
            candidate (str): The normalized candidate grade.
            reference (str): The normalized reference grade.

        Returns:
            float: The match score based on substring length and penalties.
        """
        common_length_count, _ = GradeCoatingExtractor.longest_common_substring_length(
            candidate, reference
        )

        # Score is proportional to the length of the common substring
        base_score = common_length_count / len(reference) if len(reference) > 0 else 0

        # Penalize if the match is too small compared to the candidate
        if common_length_count < len(candidate) * 0.2:
            base_score *= 0.5  # Reduce score by 50%

        return base_score

    @staticmethod
    def calculate_match_score(candidate: str, reference: str) -> float:
        """
        Calculate match score between candidate and reference string.
        Handles both prefix and substring matches, prioritizing complete reference matches.

        Args:
            candidate: String to match (e.g., 'banddx53dz150mbo406x2', 'S280GD +ZM200 Ma-C')
            reference: Reference string (e.g., 'dx53dz', 'S280GD')

        Returns:
            float: Score between 0 and 1
        """
        if not candidate or not reference:
            return 0.0

        # Convert both strings to lowercase for case-insensitive matching
        candidate = candidate.lower()
        reference = reference.lower()

        # Exact match gets perfect score
        if candidate == reference:
            return 1.0

        # Find the reference as a substring
        ref_index = candidate.find(reference)

        # If reference is found as a complete substring
        if ref_index != -1:
            # Calculate position penalty (prefer matches closer to start)
            position_penalty = (
                ref_index / len(candidate) * 0.3
            )  # Reduced impact of position

            # Perfect match for the reference itself
            match_quality = 1.0

            # Check if the reference characters are broken/interrupted in the candidate
            if ref_index == -1:
                # Find best partial match
                best_sequence = 0
                current_sequence = 0
                last_matched_pos = -1

                for i, char in enumerate(reference):
                    char_pos = candidate.find(char, last_matched_pos + 1)
                    if char_pos != -1:
                        if char_pos == last_matched_pos + 1:
                            current_sequence += 1
                        else:
                            best_sequence = max(best_sequence, current_sequence)
                            current_sequence = 1
                        last_matched_pos = char_pos

                best_sequence = max(best_sequence, current_sequence)
                match_quality = best_sequence / len(reference)

                # Heavy penalty for broken sequences
                match_quality *= 0.5

            # Calculate final score
            score = match_quality * (1.0 - position_penalty)

            # Length bonus: prefer longer references
            length_bonus = len(reference) / len(candidate)
            score *= (1.0 + length_bonus) / 2

            return max(0.0, min(1.0, score))

        # If reference is not found as a complete substring
        else:
            # Count matching characters in sequence
            matched_chars = 0
            last_pos = -1
            breaks = 0

            for char in reference:
                pos = candidate.find(char, last_pos + 1)
                if pos != -1:
                    if last_pos != -1 and pos != last_pos + 1:
                        breaks += 1
                    matched_chars += 1
                    last_pos = pos

            # Calculate base score from matched characters
            base_score = matched_chars / len(reference)

            # Heavy penalty for breaks in the sequence
            break_penalty = breaks / len(reference) * 0.7

            # Calculate final score
            score = base_score * (1.0 - break_penalty)

            # Significant penalty for incomplete matches
            score *= 0.3

            return max(0.0, min(1.0, score))

    def find_best_match(
        self, candidate: str, reference_list: List[str], threshold: float
    ) -> Tuple[Optional[int], Optional[str], bool, float]:
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

        for index, reference in enumerate(reference_list):
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
