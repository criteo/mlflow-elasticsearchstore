import pytest
import mock
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

from mlflow.tracking import MlflowClient

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import ExperimentIndex, RunIndex


@pytest.fixture
def create_store():
    IndicesClient.create = mock.MagicMock()
    ExperimentIndex.init = mock.MagicMock()
    RunIndex.init = mock.MagicMock()
    store = ElasticsearchStore("elasticsearch://store_uri", "artifact_uri")
    return store


@pytest.fixture
def create_mlflow_client():
    client = MlflowClient("elasticsearch://tracking_uri", "registry_uri")
    return client


@pytest.fixture
def init_store():
    return ElasticsearchStore(store_uri="elasticsearch://elastic:password@localhost:9200",
                              artifact_uri="viewfs://preprod-pa4/user/mlflow/mlflow_artifacts")
