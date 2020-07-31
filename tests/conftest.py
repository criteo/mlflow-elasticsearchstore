import pytest
import mock
from elasticsearch_dsl import Index, Document, connections

from mlflow.entities import RunStatus, LifecycleStage

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import (ElasticRun, ElasticMetric, ElasticParam, ElasticTag)

run = ElasticRun(meta={'id': "1"},
                 experiment_id="experiment_id", user_id="user_id",
                 status=RunStatus.to_string(RunStatus.RUNNING),
                 start_time=1, end_time=None,
                 lifecycle_stage=LifecycleStage.ACTIVE, artifact_uri="artifact_location",
                 metrics=[ElasticMetric(key="metric1", value=1, timestamp=1, step=1)],
                 params=[ElasticParam(key="param1", value="val1")],
                 tags=[ElasticTag(key="tag1", value="val1")],)


@pytest.fixture
def create_store():
    connections.create_connection = mock.MagicMock()
    store = ElasticsearchStore("user", "password", "host", "port")
    return store


@pytest.fixture
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
def get_run(elastic_run_get_mock):
    elastic_run_get_mock.return_value = run
    elastic_run_get_mock.return_value.save = mock.MagicMock()
    return elastic_run_get_mock
