from abc import ABC, abstractmethod
from typing import TextIO, BinaryIO
import datetime

import pandas as pd


class BaseScript(ABC):
    def __init__(self, start_date: datetime.date, end_date: datetime.date):
        self.start_date = start_date
        self.end_date = end_date
        self.filename = "N/A"
        self.now = "N/A"

    @abstractmethod
    def run(self, file_obj: TextIO | BinaryIO) -> pd.DataFrame:
        pass
