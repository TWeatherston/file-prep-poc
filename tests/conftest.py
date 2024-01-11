from unittest.mock import AsyncMock, MagicMock, patch
import datetime
from pathlib import Path

import pytest
import pandas as pd
from m2m_base_client.clients import DataCatalogueClient

from src.scripts.base import BaseScript
from src.preparer import FilePreparer
from src.file_handlers import BasicFileHandler
from src.settings import settings


@pytest.fixture(autouse=True)
def tmp_output_dir(tmp_path):
    tmp_output_dir = f"file://{tmp_path}"
    settings.output_dir = tmp_output_dir
    return tmp_output_dir


@pytest.fixture
def mock_catalogue_client():
    return AsyncMock(spec=DataCatalogueClient)


@pytest.fixture
def mock_base_script():
    mock_script = MagicMock(spec=BaseScript)
    mock_script.return_value.run.return_value = pd.DataFrame()
    return mock_script


@pytest.fixture
def file_preparer(mock_base_script) -> FilePreparer:
    preparer = FilePreparer(
        script_cls=mock_base_script,
        file_handler=BasicFileHandler(),
        feed_identifier="test_identifier",
        feed_version=0,
        data_supplier="test_supplier",
        start_date=datetime.date(2021, 1, 1),
        end_date=datetime.date(2021, 1, 1),
        source_creation_timestamp=datetime.datetime(2021, 1, 1),
    )
    with patch("src.preparer.datetime") as mock_datetime:
        mock_datetime.datetime.utcnow.return_value = datetime.datetime(
            2023, 12, 14, 14, 24
        )

        yield preparer


@pytest.fixture()
def get_file():
    def _(file_path: str):
        return Path(__file__).parent / "sample_files" / file_path

    return _
