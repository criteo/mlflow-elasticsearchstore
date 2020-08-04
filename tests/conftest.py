import pytest
import mock
from elasticsearch_dsl import connections

from mlflow.tracking import MlflowClient

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore


@pytest.fixture
def create_store():
    connections.create_connection = mock.MagicMock()
    store = ElasticsearchStore("file-plugin://store_uri", "artifact_uri")
    return store


@pytest.fixture
def create_mlflow_client():
    client = MlflowClient("file-plugin://tracking_uri", "registry_uri")
    return client
