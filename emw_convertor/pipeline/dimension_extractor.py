import pandas as pd
import re
import logging

# Configure logging
logger = logging.getLogger("<EMW SLExA ETL>")


class DimensionExtractor:
    """
    A class to extract dimensions (thickness, width, height) from material descriptions
    and add them as separate columns to a DataFrame.
    """

    def __init__(self, column_name: str):
        """
        Initialize the DimensionExtractor.

        Args:
            column_name (str): The name of the column containing material descriptions.
        """
        self.column_name = column_name

    def convert_str(self, val1, val2, val3):
        # Convert each value, replacing dots with commas
        val1_converted = str(val1).strip().replace(".", ",") if val1 else ""
        val2_converted = str(val2).strip().replace(".", ",") if val2 else ""
        val3_converted = str(val3).strip().replace(".", ",") if val3 else ""

        return val1_converted, val2_converted, val3_converted

    def parse_dimensions(self, description: str) -> tuple:
        """
        Parse dimensions (thickness, width, height) from a material description.

        Args:
            description (str): Material description string.

        Returns:
            tuple: (thickness, width, height) as floats or None if not found.
        """
        try:
            # Normalize the description
            if "." in description and "," in description:
                description = (
                    description.replace(".", "")
                    .replace("mm", "")
                    .replace("±", "+/-")
                    .replace(",", ".")
                )  # Convert commas to dots
            else:
                description = (
                    description.replace("mm", "").replace("±", "+/-").replace(",", ".")
                )  # Convert commas to dots
            # description = re.sub(r"\s+", "", description)  # Remove spaces

            # Remove tolerances (e.g., "+/- 0.08", "+0/-1" , "-0,12")
            # description = re.sub(
            #     r"\+/-\s*[\d\.]+", "", description
            # )  # Remove +/- tolerances

            description = re.sub(
                r"(\+/-|[+-]\d*[.,]?\d*\s*[+-/]?)\s*\d*[.,]?\d*", "", description
            )

            description = re.sub(
                r"\+[\d\.]+/-[\d\.]+", "", description
            )  # Remove +X/-Y tolerances

            description = re.sub(r"\(.*?\)", "", description)

            # description = re.sub(r"\s+", "", description)

            # Regex to capture numbers around 'x' or '*', ignoring others
            # pattern = re.compile(r"(\d+\.?\d*)[x\*](\d+\.?\d*)(?:[x\*](\d+\.?\d*))?")
            pattern = re.compile(
                r"(\d+\.?\d*)\s*[xX\*]\s*(\d+\.?\d*)(?:\s*[xX\*]\s*(\d+\.?\d*))?"
            )

            match = pattern.search(description)
            if match:
                thickness = float(match.group(1).strip()) if match.group(1) else None
                width = float(match.group(2).strip()) if match.group(2) else None
                height = float(match.group(3).strip()) if match.group(3) else None

                # Ensure at least two dimensions exist
                if thickness and width:
                    if thickness < width:
                        logger.info(
                            f"Candidate dimension '{description}' split into ({thickness}, {width}, {height})"
                        )

                        return self.convert_str(thickness, width, height)
                    else:
                        logger.info(
                            f"Candidate dimension '{description}' split into ({width}, {thickness}, {height})"
                        )

                        return self.convert_str(width, thickness, height)
                else:
                    return None, None, None

            # No valid dimensions found
            return None, None, None
        except Exception as e:
            logger.error(f"Error parsing dimensions: '{description}' | {e}")
            return None, None, None

    def extract_dimensions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract dimensions (thickness, width, height) from the specified column and add them
        as new columns in the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing material descriptions.

        Returns:
            pd.DataFrame: The updated DataFrame with three new columns: 'Dicke', 'Breit', and 'Länge'.
        """
        try:
            # Apply parsing to each row in the specified column
            df[["Dicke_", "Breit_", "Länge_"]] = df[self.column_name].apply(
                lambda x: pd.Series(self.parse_dimensions(str(x)))
            )
            logger.info(
                f"Dimensions extracted successfully for column '{self.column_name}'."
            )
            return df
        except Exception as e:
            logger.error(
                f"Error extracting dimensions for column '{self.column_name}': {e}"
            )
            raise
