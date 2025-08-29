import logging
import re

import pandas as pd

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

    def extract_tolerances(self, description: str) -> tuple:
        """
        Extract tolerance information from a material description.

        Args:
            description (str): Material description string.

        Returns:
            tuple: (thickness_tolerance, width_tolerance, height_tolerance) as strings or None if not found.
        """
        try:
            # Normalize the description for tolerance extraction
            normalized_desc = description.replace("±", "+/-").replace("mm", "")

            # Patterns to match different tolerance formats
            tolerance_patterns = [
                r"\(([+\-0-9/.,\s]+)\)",  # Tolerances in parentheses: (+0/-0,4)
                r"(\+/-\s*[\d.,]+)",  # Simple +/- tolerances: +/-0,02
                r"(\+[\d.,]+/-[\d.,]+)",  # +X/-Y format: +0/-0,4
                r"([+-][\d.,]+)",  # Simple +/- values: +0.1, -0.2
            ]

            tolerances = []
            temp_desc = normalized_desc

            # Extract all tolerance patterns
            for pattern in tolerance_patterns:
                matches = re.findall(pattern, temp_desc)
                for match in matches:
                    if match.strip():
                        tolerances.append(match.strip())
                        # Remove found tolerance from temp description
                        temp_desc = temp_desc.replace(match, "", 1)

            # Try to match tolerances to dimensions based on position
            # Look for dimension patterns with tolerances more precisely
            # Pattern to capture: number (tolerance) [text] x number (tolerance) x number (tolerance)
            # This handles cases like "0,8 nach Norm x 206 +0/-0,5"
            dimension_tolerance_pattern = r"(\d+[.,]?\d*)\s*(\([^)]+\)|\+[^x\s]*|-[^x\s]*|\+/-[^x\s]*|±[^x\s]*)?\s*(?:[a-zA-Z\s]*?)?\s*[xX\*]\s*(\d+[.,]?\d*)\s*(\([^)]+\)|\+[^\s]*|-[^\s]*|\+/-[^\s]*|±[^\s]*)?(?:\s*[xX\*]\s*(\d+[.,]?\d*)\s*(\([^)]+\)|\+[^\s]*|-[^\s]*|\+/-[^\s]*|±[^\s]*))?"

            match = re.search(dimension_tolerance_pattern, normalized_desc)

            thickness_tolerance = None
            width_tolerance = None
            height_tolerance = None

            if match:
                # Get dimensions to determine which is thickness vs width
                dim1 = (
                    float(match.group(1).replace(",", ".")) if match.group(1) else None
                )
                dim2 = (
                    float(match.group(3).replace(",", ".")) if match.group(3) else None
                )

                # Extract tolerances based on position
                tol1 = (
                    self._clean_tolerance(match.group(2))
                    if match.group(2) and match.group(2).strip()
                    else None
                )
                tol2 = (
                    self._clean_tolerance(match.group(4))
                    if match.group(4) and match.group(4).strip()
                    else None
                )
                tol3 = (
                    self._clean_tolerance(match.group(6))
                    if match.group(6) and match.group(6).strip()
                    else None
                )

                # Assign tolerances based on dimension ordering (smaller dimension = thickness)
                if dim1 and dim2:
                    if dim1 < dim2:
                        # dim1 is thickness, dim2 is width
                        thickness_tolerance = tol1
                        width_tolerance = tol2
                        height_tolerance = tol3
                    else:
                        # dim2 is thickness, dim1 is width
                        thickness_tolerance = tol2
                        width_tolerance = tol1
                        height_tolerance = tol3
                else:
                    # Fallback to positional assignment
                    thickness_tolerance = tol1
                    width_tolerance = tol2
                    height_tolerance = tol3

            # If no positional tolerances found, try to assign from general tolerance list
            if (
                not any([thickness_tolerance, width_tolerance, height_tolerance])
                and tolerances
            ):
                # Assign tolerances in order found
                if len(tolerances) >= 1:
                    thickness_tolerance = self._clean_tolerance(tolerances[0])
                if len(tolerances) >= 2:
                    width_tolerance = self._clean_tolerance(tolerances[1])
                if len(tolerances) >= 3:
                    height_tolerance = self._clean_tolerance(tolerances[2])

            return thickness_tolerance, width_tolerance, height_tolerance

        except Exception as e:
            logger.error(f"Error extracting tolerances from '{description}': {e}")
            return None, None, None

    def _clean_tolerance(self, tolerance_str: str) -> str | None:
        """
        Clean and normalize tolerance string.

        Args:
            tolerance_str (str): Raw tolerance string

        Returns:
            str: Cleaned tolerance string
        """
        if not tolerance_str:
            return None

        # Remove extra spaces and parentheses
        cleaned = tolerance_str.strip().strip("()")

        # Normalize common patterns
        cleaned = cleaned.replace("±", "+/-")

        # Remove 'mm' if present
        cleaned = cleaned.replace("mm", "").strip()

        # Return None if empty after cleaning
        return cleaned if cleaned else None

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

            # Regex to capture numbers around 'x' or '*', allowing text between dimensions
            # This handles cases like "0,8 nach Norm x 206" where there's text between dimensions
            pattern = re.compile(
                r"(\d+[.,]?\d*)\s*(?:[a-zA-Z\s]*?)?\s*[xX\*]\s*(\d+[.,]?\d*)(?:\s*[xX\*]\s*(\d+[.,]?\d*))?"
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
        Extract dimensions (thickness, width, height) and tolerances from the specified column
        and add them as new columns in the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing material descriptions.

        Returns:
            pd.DataFrame: The updated DataFrame with dimension columns and tolerance columns (if tolerances found).
        """
        try:
            # Apply dimension parsing to each row in the specified column
            df[["Dicke_", "Breit_", "Länge_"]] = df[self.column_name].apply(
                lambda x: pd.Series(self.parse_dimensions(str(x)))
            )

            # Apply tolerance extraction to each row in the specified column
            tolerance_data = df[self.column_name].apply(
                lambda x: pd.Series(self.extract_tolerances(str(x)))
            )

            # Check if any tolerances were found
            has_tolerances = tolerance_data.notna().any().any()

            if has_tolerances:
                # Add tolerance columns with German names
                df["Dickentoleranz_"] = tolerance_data.iloc[:, 0]  # Thickness tolerance
                df["Breitentoleranz_"] = tolerance_data.iloc[:, 1]  # Width tolerance
                df["Längentoleranz_"] = tolerance_data.iloc[:, 2]  # Height tolerance

                logger.info(
                    f"Dimensions and tolerances extracted successfully for column '{self.column_name}'. "
                    f"Tolerance columns added."
                )
            else:
                logger.info(
                    f"Dimensions extracted successfully for column '{self.column_name}'. "
                    f"No tolerances found - tolerance columns not added."
                )

            return df
        except Exception as e:
            logger.error(
                f"Error extracting dimensions for column '{self.column_name}': {e}"
            )
            raise
