import datetime

from pydantic import BaseModel, Field, AnyUrl, field_serializer


class PrepareFile(BaseModel):
    feed_identifier: str = Field(
        ..., description="What type of file this is. E.g. retaillink_daily_sales"
    )
    feed_version: int = Field(
        1,
        description="What preparation script version to use when processing the file",
    )
    file_location: AnyUrl = Field(
        ...,
        description="Location of the file. Should be in the URI format",
        examples=["s3://bucket-name/path/to/file.csv", "file:///path/to/file.csv"],
    )
    data_supplier: str = Field(
        ...,
        description="Name of the data supplier.",
    )
    start_date: datetime.date = Field(
        ...,
        description="Start date of the data contained in the file (inclusive)",
    )
    end_date: datetime.date = Field(
        ...,
        description="End date of the data contained in the file (inclusive)",
    )
    source_creation_timestamp: datetime.datetime = Field(
        ...,
        description="Timestamp of when the file was created by the data supplier",
    )
    concat: bool = Field(
        True,
        description="Whether to concatenate the preparation results into a single output file",
    )

    @field_serializer("file_location")
    def serialize_file_location(self, location: AnyUrl) -> str:
        """Serializes the file location to a string so that it can be passed to Celery"""
        return str(location)


class TaskResult(BaseModel):
    task_id: str = Field(..., description="ID of the Celery task")
