from typing import TextIO, BinaryIO

import pandas as pd

from src.scripts.base import BaseScript
from src.factories import preparer_factory
from src.file_handlers import BasicFileHandler


@preparer_factory.register("waitroseconnect_daily_line_sales", 1, BasicFileHandler())
class DailyLineSales(BaseScript):
    def run(self, file_obj: TextIO | BinaryIO) -> pd.DataFrame:
        columns = [
            "Day",
            "Date",
            "Line",
            "Line_Description",
            "Registered_Sales",
            "Sales_SUs",
            "Reduced",
            "Explained_Wastage",
            "Explained_Wastage_Quality",
            "Reductions_pct_Registered_Sales",
        ]

        df = pd.read_csv(file_obj)
        df = self._deduplicate_single_daily_line_sales(df)

        pound_columns = [
            "Registered_Sales",
            "Reduced",
            "Explained_Wastage",
            "Explained_Wastage_Quality",
        ]
        for column in pound_columns:
            df[column] = df[column].str.replace("£", "")

        return df[columns]

    @staticmethod
    def _deduplicate_single_daily_line_sales(df: pd.DataFrame) -> pd.DataFrame:
        """Handles the case where the single daily line sales file is completely duplicated (including the header)"""
        deduplicated_df = df[df.Date != "Date"]  # Remove extra header row
        deduplicated_df.drop_duplicates(inplace=True)
        return deduplicated_df
