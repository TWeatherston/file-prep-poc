from collections import OrderedDict
from typing import TextIO, BinaryIO

import pandas as pd
import numpy as np

from src.scripts.base import BaseScript
from src.factories import preparer_factory
from src.file_handlers import SeparatedFileHandler


@preparer_factory.register(
    feed="horizon_daily_performance_sales",
    version=1,
    opener=SeparatedFileHandler(encoding="latin1"),
)
class DailySales(BaseScript):
    def run(self, file_obj: TextIO | BinaryIO) -> pd.DataFrame:
        column_headers = [
            "SKU",
            "DEPTCOMM",
            "DESCRIPTION",
            "SALESCASH",
            "SALESVOL",
            "WASTAGETOTALVAL",
            "WASTAGETOTALVOL",
            "WASTAGETOTALPCT",
            "AVAILINSTPCT",
            "AVAILVOLPCT",
            "STORESTOCK",
            "MAINSTOCKAVAILABLE",
            "MAINSTOCKHELD",
            "PCCSTOCK",
            "LYINGOUT",
            "BONDSTOCK",
            "DEPOTISSUES",
            "DEPOTSERVICEPCT",
            "SUPPSERVNUMBER",
            "SUPPSERVPCT",
            "LOSTSALESVOL",
            "LOSTSALESVAL",
            "NOREPLEN",
            "VICTIMIND",
            "PROMOIND",
        ]

        data_frame = pd.read_csv(file_obj, dtype=str, names=column_headers)

        dest_header = OrderedDict(
            [
                ("ID", float),
                ("SOURCEFILENAME", str),
                (
                    "PROCESSED",
                    None,
                ),
                ("RETAILER", str),
                ("DAILY", str),
                (
                    "CATEGORY",
                    str,
                ),
                ("SKU", float),
                (
                    "DEPTCOMM",
                    str,
                ),
                (
                    "DESCRIPTION",
                    str,
                ),
                ("LOSTOPP", float),
                ("SALESCASH", float),
                ("SALESVOL", float),
                ("LOSTSALESVAL", float),
                ("LOSTSALESVOL", float),
                ("WASTAGETOTALVAL", float),
                ("WASTAGETOTALVOL", float),
                ("WASTAGETOTALPCT", float),
                ("AVAILINSTPCT", float),
                ("AVAILVOLPCT", float),
                ("STORESTOCK", float),
                ("MAINSTOCKAVAILABLE", float),
                ("MAINSTOCKHELD", float),
                ("PCCSTOCK", float),
                ("LYINGOUT", float),
                ("BONDSTOCK", float),
                ("DEPOTISSUES", float),
                ("DEPOTSERVICEPCT", float),
                ("SUPPSERVNUMBER", float),
                ("SUPPSERVPCT", float),
                ("STORESRANGED", float),
                ("NOREPLEN", float),
                (
                    "VICTIMIND",
                    str,
                ),
                ("PROMOIND", str),
            ]
        )

        data_frame = data_frame.loc[~data_frame["SKU"].isnull(), :].iloc[1:, :]

        date_rows = data_frame.SKU.str.contains(r"^\d{2}\/\d{2}\/\d{4}$").fillna(False)
        sku_rows = data_frame.SKU.str.contains(r"^\d+$").fillna(False)
        label_rows = data_frame.SKU.isin(
            ["Sub-Cat Subtotal", "Overall Sub-Cat Total"]
        ).fillna(False)
        category_rows = ~label_rows & ~sku_rows & ~date_rows

        data_frame["DAILY"] = 0
        data_frame.loc[date_rows, "DAILY"] = data_frame.loc[date_rows, "SKU"]
        data_frame["DAILY"] = data_frame["DAILY"].replace(to_replace=0, method="ffill")
        data_frame["DAILY"] = data_frame.DAILY.str.replace(
            r"(\d{2})\/(\d{2})\/(\d{4})", r"\3\2\1"
        )

        data_frame["CATEGORY"] = 0
        data_frame.loc[category_rows, "CATEGORY"] = data_frame.loc[category_rows, "SKU"]
        data_frame["CATEGORY"] = data_frame["CATEGORY"].replace(
            to_replace=0, method="ffill"
        )
        data_frame = data_frame.loc[sku_rows]

        data_frame["SOURCEFILENAME"] = self.filename
        data_frame["PROCESSED"] = self.now

        data_frame["ID"] = list(range(len(data_frame)))

        for col in dest_header:
            if col not in data_frame:
                data_frame[col] = None
            elif dest_header[col] is not None:
                data_frame[col] = data_frame[col].astype(dest_header[col])

        for c in data_frame.select_dtypes(include=["object"]).columns:
            data_frame[c] = data_frame[c].replace("nan", np.nan)

        data_frame = data_frame[list(dest_header.keys())]

        return data_frame
