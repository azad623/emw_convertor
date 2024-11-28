import logging
import logging.config
import yaml
import os
from datetime import datetime
from typing import Optional
from emw_convertor import log_config_path, log_output_path


class CustomLogger(logging.Logger):
    def __init__(self, name, log_file, config):
        super().__init__(name)

        # Load YAML configuration
        with open(log_config_path, "r") as file:
            logging_cfg = yaml.safe_load(file)

        # Set up log file paths
        base_name = os.path.basename(log_file).split(".")[0]
        info_out_path = os.path.join(log_output_path, f"{base_name}.info.log")
        error_out_path = os.path.join(log_output_path, f"{base_name}.error.log")

        # Remove existing log files if rewrite is enabled
        # if config["etl_pipeline"].get("rewrite_log", False):
        #    for path in [info_out_path, error_out_path]:
        #        if os.path.exists(path):
        #            os.remove(path)

        # Update handler paths in config
        logging_cfg["handlers"]["info_file_handler"]["filename"] = info_out_path
        logging_cfg["handlers"]["error_file_handler"]["filename"] = error_out_path
        logging_cfg["handlers"]["warning_file_handler"]["filename"] = error_out_path

        # Apply configuration
        logging.config.dictConfig(logging_cfg)
        self.info_handler = logging.FileHandler(info_out_path)
        self.error_handler = logging.FileHandler(error_out_path)

    def info(self, msg, *args, to_terminal=True, **kwargs):
        """
        Logs an info-level message with optional terminal output.

        Args:
            msg (str): The message to log.
            to_terminal (bool): If True, log message is also printed to the terminal.
        """
        # Log to file
        super().info(msg, *args, **kwargs)

        # Log to terminal if specified
        if to_terminal:
            console_handler = logging.StreamHandler()
            self.addHandler(console_handler)
            super().info(msg, *args, **kwargs)
            self.removeHandler(console_handler)


def setup_logger(file_path: str, config: dict) -> CustomLogger:
    """
    Set up a custom logger with file and optional terminal logging.

    Args:
        file_path (str): The base path of the file being processed.
        config (dict): Configuration dictionary with logging settings.

    Returns:
        CustomLogger: Configured custom logger.
    """
    return CustomLogger("<EMW SLExA ETL>", file_path, config)
