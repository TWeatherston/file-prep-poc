import datetime

from .celery import app

from .factories import preparer_factory
from .data_catalogue import data_catalogue_client


@app.task
def prepare_file(
    feed_identifier: str,
    feed_version: int,
    file_location: str,
    data_supplier: str,
    start_date: datetime.date,
    end_date: datetime.date,
    source_creation_timestamp: datetime.datetime,
    concat: bool = True,
):
    """Prepare a file for ingestion into the desire platform.

    Args:
        feed_identifier (str): What type of file this is. E.g. retaillink_daily_sales
        feed_version (int): What version of the preparation script is required to process the file
        file_location (str): Where is the file located
        data_supplier (str): Who supplied the file
        start_date (datetime.date): Start date of the data in the file
        end_date (datetime.date): End date of the data in the file
        source_creation_timestamp (datetime.datetime): When was the file created
        concat (bool, optional): Should the resulting dataframes be concatenated into one. Defaults to True.
    """
    file_preparer = preparer_factory.create(
        feed_identifier,
        feed_version,
        data_supplier,
        start_date,
        end_date,
        source_creation_timestamp,
    )
    dataframes = file_preparer.run_script(file_location)
    file_preparer.write_outputs_to_file(dataframes, concat)
    file_preparer.catalogue_outputs(data_catalogue_client())
