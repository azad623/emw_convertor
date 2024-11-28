import logging
from typing import Optional, List, Tuple
import pandas as pd
from emw_convertor import global_vars, schema

logger = logging.getLogger("<EMW SLExA ETLL>")


def calculate_match_ratio(base_grade: str, cell_value: str) -> float:
    """
    Calculates a match ratio between a base_grade and a cell_value with penalties
    for exact matches or very small ratios.

    Args:
        base_grade (str): The base grade to match.
        cell_value (str): The cell value to compare against.

    Returns:
        float: The calculated match ratio after applying penalties.
    """
    # Normalize inputs
    base_grade = base_grade.replace(" ", "").lower()
    cell_value = cell_value.replace(" ", "").lower()

    # Avoid division by zero
    if len(cell_value) == 0:
        return 0.0

    # Calculate the base match ratio
    match_ratio = len(base_grade) / len(cell_value)

    # Apply penalties directly to the ratio
    if match_ratio == 1:
        # Penalize exact matches (perfect matches often less meaningful)
        match_ratio *= 0.5
    elif match_ratio < 0.2:
        # Penalize very small ratios (base_grade matches very long text)
        match_ratio *= 0.7

    # Ensure the ratio is non-negative
    return max(match_ratio, 0.0)


def identify_header_name(
    df: pd.DataFrame, file_path, threshold: float = 0.1
) -> Tuple[bool, Optional[str], float]:
    """
    Identifies the column name in the DataFrame that best matches the 'base_grade' from the schema.

    Args:
        df (pd.DataFrame): The input DataFrame containing raw data.
        base_grades (List[str]): A list of base grades extracted from the schema.
        threshold (float): Minimum score required to consider a match valid. Default is 0.7.

    Returns:
        Tuple[bool, Optional[str], float]: A tuple containing:
            - A boolean indicating whether a valid match was found.
            - The best-matching column name (or None if no match is found).
            - The match score for the best column.

    Raises:
        ValueError: If no valid match is found.
    """
    try:
        max_score = 0
        best_column = None

        base_grades: List[str] = [
            item["base_grade"].replace(" ", "").lower() for item in schema
        ]

        # Iterate through DataFrame columns
        score_list = []
        for col in df.columns:
            column_score = 0
            total_penalized_ratio = 0

            # Loop through rows in the column
            for cell in df[col]:
                if pd.isna(cell):  # Skip NaN values
                    continue

                # Normalize the cell value
                cell_value = str(cell).replace(" ", "").lower()

                # Check for matches and calculate penalized ratios
                for base_grade in base_grades:
                    if len(base_grade) > 3:  # Avoid very short base grades
                        if base_grade in cell_value:
                            penalized_ratio = calculate_match_ratio(
                                base_grade, cell_value
                            )
                            column_score += 1
                            total_penalized_ratio += penalized_ratio
                            break

            # Compute the final score for the column
            if column_score > 0:
                final_score = (
                    total_penalized_ratio / column_score if column_score > 0 else 0
                )
                score_list.append([col, final_score])

                # Update the best-matching column if the score is higher
                if final_score > max_score:
                    max_score = final_score
                    best_column = col

        # Check if the max score meets the threshold
        if max_score < threshold:
            error_message = (
                f"Could not find a proper column. Maximum score {max_score:.2f} "
                f"did not meet the threshold of {threshold}."
            )
            global_vars["error_list"].append(error_message)
            logger.error(error_message)
            return False, None, max_score
        print(score_list)
        return True, best_column, max_score

    except Exception as e:
        logger.error(f"An error occurred during column identification: {e}")
        raise RuntimeError(f"An unexpected error occurred: {e}") from e
