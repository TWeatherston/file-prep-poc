from m2m_base_client.clients import DataCatalogueClient

from src.data_catalogue import data_catalogue_client, request_file_catalogue


def test_data_catalogue_client():
    client = data_catalogue_client()
    assert isinstance(client, DataCatalogueClient)
    assert client.authorization_base_url == "https://auth0.test.com/authorize"
    assert client.audience == "https://data-catalogue.test.skutrak.com"
    assert client.root_url == "https://data-catalogue.test.skutrak.com/"
    assert client.client_id == "auth0_test_client_id"
    assert client.client_secret == "auth0_test_client_secret"


def test_request_file_catalogue(mock_catalogue_client):
    request_file_catalogue(mock_catalogue_client, [])
    mock_catalogue_client.post.assert_called_once_with(
        "catalogue/",
        file_records=[[]],
        flush=False,
    )
