"""bilstein_slexa."""

import os
import json
import logging
import logging.config
from pathlib import Path
from typing import Optional
import yaml


def get_yaml_config(file_path: Path) -> Optional[dict]:
    """Fetch yaml config and return as dict if it exists."""
    if file_path.exists():
        with open(file_path, "rt") as f:
            return yaml.load(f.read(), Loader=yaml.FullLoader)


def load_layout_schema(schema_path: str) -> dict | None:
    """Load layout schema
    Args:
        schema_path (str): Path to the json schema

    Returns:
        dict : parsed json file
    """
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)

        if not isinstance(schema, list):
            raise ValueError("The schema must be a list of column definitions.")

        print("Schema successfully loaded and validated.")
        return schema

    except FileNotFoundError:
        print(f"Schema file not found: {schema_path}")
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON file: {e}")
    except ValueError as e:
        print(f"Schema validation error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while loading the schema: {e}")


# Define project base directory
PROJECT_DIR = Path(__file__).resolve().parents[1]


# Collect errors
global_vars = {"error_list": []}

# Define log output locations
log_output_path = str(Path(__file__).parent.resolve() / "logs/")

# Define path to material json file
_material_schema_path = str(
    Path(__file__).parent.resolve() / "config/schemas/emw_material_kb.json"
)

schema = load_layout_schema(_material_schema_path)

# Read log config file
log_config_path = Path(__file__).parent.resolve() / "config/logging.yaml"

# Define module logger
logger = logging.getLogger(__name__)

# base/global config
_base_config_path = Path(__file__).parent.resolve() / "config/base.yaml"
config = get_yaml_config(_base_config_path)

# define input and output data locations
local_data_input_path = str(Path(__file__).resolve().parents[1] / "inputs/")
local_data_output_path = str(Path(__file__).resolve().parents[1] / "outputs/")
