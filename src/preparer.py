import os
import csv
import datetime
from typing import Type

import pandas as pd
import fsspec
from m2m_base_client.clients import DataCatalogueClient
from m2m_base_client.models import CatalogueFileRecord

from .scripts.base import BaseScript
from .settings import settings
from .file_handlers import FileHandler
from .data_catalogue import request_file_catalogue


class FilePreparer:
    def __init__(
        self,
        script_cls: Type[BaseScript],
        file_handler: FileHandler,
        feed_identifier: str,
        feed_version: int,
        data_supplier: str,
        start_date: datetime.date,
        end_date: datetime.date,
        source_creation_timestamp: datetime.datetime,
    ):
        self.file_handler = file_handler
        self.script = script_cls(start_date, end_date)

        self.feed_identifier = feed_identifier
        self.feed_version = feed_version
        self.data_supplier = data_supplier
        self.start_date = start_date
        self.end_date = end_date
        self.source_creation_timestamp = source_creation_timestamp

        self.files_counter = 0
        self.output_file_paths = []

    def run_script(self, file_path: str) -> list[pd.DataFrame]:
        """Generates a list of dataframes by calling the script's run method on each file in the file path.

        Args:
            file_path (str): Path to the file that needs to be prepared

        Returns:
            list[pd.DataFrame]: List of dataframes
        """
        return [self.script.run(f) for f in self.file_handler.open(file_path)]

    def write_outputs_to_file(
        self, dataframes: list[pd.DataFrame], concat: bool = True
    ) -> list[str]:
        """Writes a list of dataframes to a file and returns the file location. All files are encoded in utf8, with
        no index. If concat is True, then concatenate all dataframes into a single dataframe before writing to file.
        """
        if concat:
            df = pd.concat(dataframes)
            self.output_file_paths.append(self._write_dataframe_to_file(df))
        else:
            for df in dataframes:
                self.output_file_paths.append(self._write_dataframe_to_file(df))
        return self.output_file_paths

    def _write_dataframe_to_file(self, output_df: pd.DataFrame) -> str:
        """Writes a dataframe to a file and returns the file location. All files are encoded in utf8, with no index

        Args:
            output_df (pd.DataFrame): Dataframe to write to file

        Returns:
            str: File location
        """
        output_path = self._generate_file_path()
        with fsspec.open(output_path, "w") as f:
            output_df.to_csv(f, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf8")
        return output_path

    def _generate_file_path(self) -> str:
        """Generates a file path for the output file. The file path is generated based on the output path and the
        files counter, which is incremented each time a file is generated. This helps to facilitate generating multiple
        output files from a single input file."""
        output_path = os.path.join(
            settings.output_dir,
            self.data_supplier,
            datetime.datetime.utcnow().strftime("%Y-%m-%d %H-%M-%S-%f"),
            self.feed_identifier,
        )
        file_path = f"{output_path}.{self.files_counter}.csv"
        self.files_counter += 1
        return file_path

    def catalogue_outputs(self, client: DataCatalogueClient):
        """Catalogues all output files.

        Args:
            client (DataCatalogueClient): Data catalogue client
        """
        request_file_catalogue(
            client=client,
            file_records=[
                CatalogueFileRecord(
                    feed_identifier=self.feed_identifier,
                    feed_version=self.feed_version,
                    file_location=file_path,
                    organisation="atheon",
                    file_meta={
                        "start_date": self.start_date.isoformat(),
                        "end_date": self.end_date.isoformat(),
                        "data_provider": self.data_supplier,
                        "source_creation_timestamp": self.source_creation_timestamp.isoformat(),
                    },
                )
                for file_path in self.output_file_paths
            ],
        )
