import pytest
import mock
from elasticsearch_dsl import connections

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore


@pytest.fixture
def create_store():
    connections.create_connection = mock.MagicMock()
    store = ElasticsearchStore("user", "password", "host", "port")
    return store
