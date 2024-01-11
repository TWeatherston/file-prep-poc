import datetime
from typing import Type

from src.scripts.base import BaseScript
from src.file_handlers import FileHandler, BasicFileHandler
from src.preparer import FilePreparer


class PreparerFactory:
    """Factory class that can be used to create instances of FilePreparer"""

    _registry: dict[tuple[str, int], tuple[Type[BaseScript], FileHandler]] = {}

    def _register(
        self,
        feed_identifier: str,
        feed_version: int,
        script: Type[BaseScript],
        file_handler: FileHandler,
    ):
        """Register a script and file handler for a given feed and version

        Args:
            feed_identifier (str): Identifier of feed
            feed_version (int): Version of feed
            script (Type[BaseScript]): Script to use for processing feed
            file_handler (FileHandler): File handler to use for opening the file. Defaults to BasicFileHandler.
        """
        self._registry[(feed_identifier, feed_version)] = (script, file_handler)

    def register(
        self, feed: str, version: int, opener: FileHandler = BasicFileHandler()
    ):
        """Decorator to register a script and file handler for a given feed and version

        Args:
            feed (str): Identifier of feed
            version (int): Version of feed
            opener (FileHandler): File handler to use for opening the file
        """

        def decorator(script: Type[BaseScript]):
            self._register(feed, version, script, opener)
            return script

        return decorator

    def create(
        self,
        identifier: str,
        version: int,
        data_supplier: str,
        start_date: datetime.date,
        end_date: datetime.date,
        source_creation_timestamp: datetime.datetime,
    ) -> FilePreparer:
        """Create a FilePreparer instance for the given feed and version

        Args:
            identifier (str): Identifier of feed
            version (int): Version of feed
            data_supplier (str): Name of the data supplier
            start_date (datetime.date): Start date of the data contained in the file (inclusive)
            end_date (datetime.date): End date of the data contained in the file (inclusive)
            source_creation_timestamp (datetime.datetime): Source creation timestamp of the file

        Returns:
            FilePreparer: Instance of FilePreparer for the given feed and version
        """
        script_cls, file_handler = self._registry[(identifier, version)]
        return FilePreparer(
            script_cls,
            file_handler,
            identifier,
            version,
            data_supplier,
            start_date,
            end_date,
            source_creation_timestamp,
        )


preparer_factory = PreparerFactory()
