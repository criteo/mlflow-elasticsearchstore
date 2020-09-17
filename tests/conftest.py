import pytest
import mock
from elasticsearch_dsl import connections

from mlflow.tracking import MlflowClient

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import ElasticExperiment, ElasticRun, ElasticMetric


@pytest.fixture
def create_store():
    connections.create_connection = mock.MagicMock()
    ElasticExperiment.init = mock.MagicMock()
    ElasticRun.init = mock.MagicMock()
    ElasticMetric.init = mock.MagicMock()
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
