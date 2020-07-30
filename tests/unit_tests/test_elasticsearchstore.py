import pytest
import mock
import mlflow
from mlflow.entities.lifecycle_stage import LifecycleStage
import elasticsearch_dsl

import mlflow_elasticsearchstore
from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import ElasticExperiment, ElasticRun, \
    ElasticMetric, ElasticParam, ElasticTag


@mock.patch('elasticsearch_dsl.Document')
def test_create_experiment(document_mock):
    store = ElasticsearchStore("user", "password", "host", "port")
    store.create_experiment("name", "artifact_location")
    document_mock.assert_called_once_with(name="name", lifecycle_stage=LifecycleStage.ACTIVE,
                                          artifact_location="artifact_location")
    experiment_mock = document_mock.return_value.__init__
    experiment_mock.assert_called_once_with()


@mock.patch('elasticsearch_dsl.Document')
def test__get_run(document_mock):
    store = ElasticsearchStore("user", "password", "host", "port")
    store._get_run(1)
    document_mock.assert_called_once_with(id=1)
    run_mock = document_mock.return_value.get
    assert run_mock.meta.id == 1
