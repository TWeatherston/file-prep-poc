from typing import TextIO, BinaryIO

import pandas as pd
import numpy as np

from src.scripts.base import BaseScript
from src.factories import preparer_factory
from src.file_handlers import BasicFileHandler


@preparer_factory.register(
    feed="retaillink_daily_sales", version=1, opener=BasicFileHandler()
)
class DailySales(BaseScript):
    def run(self, file_obj: TextIO | BinaryIO) -> pd.DataFrame:
        data_frame = pd.read_csv(file_obj, delimiter="\t", dtype=str, header=None)

        source_header = [
            "SUPPLIERNUMBER",
            "PRIMEITEMNBR",
            "STORENBR",
            "DAILY",
            "EPOSSALES",
            "EPOSQTY",
            "MAXSHELFQTY",
        ]

        dest_header = [
            "ID",
            "SOURCEFILENAME",
            "PROCESSED",
            "PRIMEITEMNBR",
            "STORENBR",
            "DAILY",
            "CURRSTRONHANDQTY",
            "CURRTRAITED",
            "EPOSSALES",
            "EPOSQTY",
            "MAXSHELFQTY",
            "SUPPLIERNUMBER",
        ]

        data_frame.columns = source_header

        # Type conversions
        conversions = [
            ("EPOSSALES", float),
            ("EPOSQTY", float),
            ("MAXSHELFQTY", float),
        ]

        for col, totype in conversions:
            data_frame[col] = data_frame[col].astype(totype)

        for c in data_frame.select_dtypes(include=["object"]).columns:
            data_frame[c] = data_frame[c].replace("nan", np.nan)

        data_frame["DAILY"] = pd.to_datetime(data_frame["DAILY"])

        data_frame["SUPPLIERNUMBER"] = data_frame.SUPPLIERNUMBER.str.lstrip("0")

        data_frame["STORENBR"] = data_frame.STORENBR.str.extract(r"(\d+)\.\d*")

        data_frame.reset_index(inplace=True)
        data_frame.rename(columns={"index": "ID"}, inplace=True)

        for col in dest_header:
            if col not in data_frame:
                data_frame[col] = None

        data_frame = data_frame[dest_header]
        return data_frame
