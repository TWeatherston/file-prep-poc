import asyncio

from m2m_base_client.clients import DataCatalogueClient
from m2m_base_client.models import CatalogueFileRecord

from src.settings import settings


class DummyCache:
    def __init__(self):
        self.cache = {}

    def get(self, key):
        return self.cache.get(key)

    def set(self, key, value, timeout):
        self.cache[key] = value


def data_catalogue_client() -> DataCatalogueClient:
    return DataCatalogueClient(
        client_id=settings.auth0.client_id,
        client_secret=settings.auth0.client_secret.get_secret_value(),
        authorization_base_url=str(settings.auth0.authorization_base_url),
        audience=str(settings.auth0.audience).rstrip("/"),
        cache=DummyCache(),
        root_url=str(settings.auth0.root_url),
        timeout=30.0,
    )


def request_file_catalogue(
    client: DataCatalogueClient,
    file_records: list[CatalogueFileRecord],
):
    asyncio.run(
        client.post(
            "catalogue/",
            file_records=[file_records],
            flush=False,
        )
    )
