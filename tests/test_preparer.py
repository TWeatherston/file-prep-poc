import datetime

from m2m_base_client.models import CatalogueFileRecord
import pandas as pd
import fsspec


def test_preparer_run_script(mock_base_script, file_preparer, tmp_path):
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("test")
    file_preparer.run_script(file_path=str(file_path))
    mock_base_script.assert_called_once_with(
        datetime.date(2021, 1, 1), datetime.date(2021, 1, 1)
    )


def test_preparer_write_outputs_to_file_concat(file_preparer, tmp_output_dir):
    df1 = pd.DataFrame([(1, 2, 3)], columns=["a", "b", "c"])
    df2 = pd.DataFrame([(4, 5, 6)], columns=["a", "b", "c"])
    file_preparer.write_outputs_to_file([df1, df2])
    assert file_preparer.output_file_paths == [
        f"{tmp_output_dir}/test_supplier/2023-12-14 14-24-00-000000/test_identifier.0.csv"
    ]
    with fsspec.open(file_preparer.output_file_paths[0], "r", encoding="utf-8") as f:
        assert f.read().splitlines() == ["a,b,c", "1,2,3", "4,5,6"]


def test_preparer_write_outputs_to_file_no_concat(file_preparer, tmp_output_dir):
    df1 = pd.DataFrame([(1, 2, 3)], columns=["a", "b", "c"])
    df2 = pd.DataFrame([(4, 5, 6)], columns=["a", "b", "c"])
    file_preparer.write_outputs_to_file([df1, df2], concat=False)
    assert file_preparer.output_file_paths == [
        f"{tmp_output_dir}/test_supplier/2023-12-14 14-24-00-000000/test_identifier.0.csv",
        f"{tmp_output_dir}/test_supplier/2023-12-14 14-24-00-000000/test_identifier.1.csv",
    ]


def test_write_dataframe_to_file(file_preparer, tmp_output_dir):
    df = pd.DataFrame([(1, 2, 3)], columns=["a", "b", "c"])
    output_path = file_preparer._write_dataframe_to_file(df)
    with fsspec.open(output_path, "r", encoding="utf-8") as f:
        assert f.read().splitlines() == ["a,b,c", "1,2,3"]


def test_preparer_generate_file_path(file_preparer, tmp_output_dir):
    file_path = file_preparer._generate_file_path()
    assert (
        file_path
        == f"{tmp_output_dir}/test_supplier/2023-12-14 14-24-00-000000/test_identifier.0.csv"
    )

    file_path = file_preparer._generate_file_path()
    assert (
        file_path
        == f"{tmp_output_dir}/test_supplier/2023-12-14 14-24-00-000000/test_identifier.1.csv"
    )


def test_preparer_catalogue_outputs(file_preparer, mock_catalogue_client):
    file_preparer.output_file_paths = [
        "file://output/test_supplier/2023-12-14 14-24-03-509409/test_identifier.0.csv",
        "file://output/test_supplier/2023-12-14 14-24-03-509409/test_identifier.1.csv",
    ]

    file_preparer.catalogue_outputs(mock_catalogue_client)

    mock_catalogue_client.post.assert_called_once_with(
        "catalogue/",
        file_records=[
            [
                CatalogueFileRecord(
                    feed_identifier="test_identifier",
                    feed_version=0,
                    file_location="file://output/test_supplier/2023-12-14 14-24-03-509409/test_identifier.0.csv",
                    organisation="atheon",
                    file_meta={
                        "start_date": "2021-01-01",
                        "end_date": "2021-01-01",
                        "data_provider": "test_supplier",
                        "source_creation_timestamp": "2021-01-01T00:00:00",
                    },
                ),
                CatalogueFileRecord(
                    feed_identifier="test_identifier",
                    feed_version=0,
                    file_location="file://output/test_supplier/2023-12-14 14-24-03-509409/test_identifier.1.csv",
                    organisation="atheon",
                    file_meta={
                        "start_date": "2021-01-01",
                        "end_date": "2021-01-01",
                        "data_provider": "test_supplier",
                        "source_creation_timestamp": "2021-01-01T00:00:00",
                    },
                ),
            ]
        ],
        flush=False,
    )
