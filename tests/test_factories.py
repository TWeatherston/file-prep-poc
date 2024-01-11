from typing import TextIO, BinaryIO
import datetime

import pandas as pd

from src.factories import PreparerFactory
from src.scripts.base import BaseScript
from src.file_handlers import BasicFileHandler
from src.preparer import FilePreparer


def test_register_method():
    factory = PreparerFactory()
    factory._register("test", 1, BaseScript, BasicFileHandler())
    script, handler = factory._registry.get(("test", 1))
    assert script == BaseScript
    assert isinstance(handler, BasicFileHandler)


def test_register_decorator():
    factory = PreparerFactory()

    @factory.register("test", 1, BasicFileHandler())
    class TestScript(BaseScript):
        def run(self, file_obj: TextIO | BinaryIO) -> pd.DataFrame:
            pass

    script, handler = factory._registry.get(("test", 1))
    assert script == TestScript
    assert isinstance(handler, BasicFileHandler)


def test_create_method(mock_base_script):
    factory = PreparerFactory()
    factory._register("test", 1, mock_base_script, BasicFileHandler())
    preparer = factory.create(
        identifier="test",
        version=1,
        data_supplier="test",
        start_date=datetime.date(2021, 1, 1),
        end_date=datetime.date(2021, 1, 1),
        source_creation_timestamp=datetime.datetime(2021, 1, 1, 0, 0, 0),
    )
    assert isinstance(preparer, FilePreparer)
    assert isinstance(preparer.file_handler, BasicFileHandler)
    assert preparer.script == mock_base_script.return_value
    assert preparer.feed_identifier == "test"
    assert preparer.feed_version == 1
    assert preparer.data_supplier == "test"
    assert preparer.start_date == datetime.date(2021, 1, 1)
    assert preparer.end_date == datetime.date(2021, 1, 1)
    assert preparer.source_creation_timestamp == datetime.datetime(2021, 1, 1, 0, 0, 0)
    assert preparer.files_counter == 0
    assert preparer.output_file_paths == []
