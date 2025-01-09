import streamlit as st
import pandas as pd
from tinydb import TinyDB, Query
from datetime import datetime
from typing import Dict, List
import os


class DashboardManager:
    def __init__(self):
        """Initialize dashboard manager with database connection"""
        self.db = TinyDB("dashboard_stats.json")
        self.files_table = self.db.table("processed_files")

    def save_process_results(self, file_info: Dict) -> None:
        """
        Save processing results to database.
        Should be called after successful processing.
        """
        try:

            if not file_info["success"]:
                return

            # Prepare record
            record = {
                "filename": file_info["filename"],
                "supplier": str(file_info.get("supplier", "unknown").capitalize()),
                "upload_date": datetime.now().isoformat(),
                "rows_processed": len(file_info["dataframe"]),
                "value_frequencies": {
                    "Güte_": self._get_frequencies(file_info["dataframe"], "Güte_"),
                    "Dicke_": self._get_frequencies(file_info["dataframe"], "Dicke_"),
                    "Breit_": self._get_frequencies(file_info["dataframe"], "Breit_"),
                },
            }

            # Save to database
            self.files_table.insert(record)

        except Exception as e:
            st.error(f"Error saving process results: {str(e)}")

    def _get_frequencies(self, df: pd.DataFrame, column: str) -> Dict:
        """Calculate value frequencies for a column"""
        if column not in df.columns:
            return {}
        return df[column].value_counts().to_dict()

    def get_dashboard_stats(self) -> Dict:
        """Get aggregated statistics for dashboard"""
        all_records = self.files_table.all()

        if not all_records:
            return {
                "total_files": 0,
                "total_rows": 0,
                "unique_supplier": 0,
                "frequencies": {},
                "file_history": pd.DataFrame(),
            }

        # Convert records to DataFrame
        history_df = pd.DataFrame(all_records)
        history_df["upload_date"] = pd.to_datetime(history_df["upload_date"])

        # Aggregate frequencies
        frequencies = {"Grade": {}, "Thickness (mm)": {}, "Width (mm)": {}}

        for record in all_records:
            for key in frequencies:
                for value, count in record["value_frequencies"][key].items():
                    frequencies[key][value] = frequencies[key].get(value, 0) + count

        return {
            "total_files": len(all_records),
            "total_rows": history_df["rows_processed"].sum(),
            "unique_supplier": history_df["supplier"].nunique(),
            "frequencies": frequencies,
            "file_history": history_df,
        }

    def reset_database(self) -> None:
        """Reset all dashboard data"""
        self.files_table.truncate()
