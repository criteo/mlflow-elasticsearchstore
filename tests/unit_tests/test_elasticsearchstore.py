import pytest
import mock
import mlflow
import elasticsearch_dsl

from mock import MagicMock
from mlflow.entities import (
    Experiment, RunTag, Metric, Param, RunData, RunInfo,
    SourceType, RunStatus, Run, ViewType, ExperimentTag, Columns, LifecycleStage)
from elasticsearch_dsl import Index, Search, Q, connections

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import ElasticExperiment, ElasticRun, \
    ElasticMetric, ElasticParam, ElasticTag

mock_experiment = ElasticExperiment(meta={'id': "1"}, name="name",
                                    lifecycle_stage=LifecycleStage.ACTIVE,
                                    artifact_location="artifact_location")

mock_run = ElasticRun(meta={'id': "1"},
                      experiment_id="experiment_id", user_id="user_id",
                      status=RunStatus.to_string(RunStatus.RUNNING),
                      start_time=1, end_time=None,
                      lifecycle_stage=LifecycleStage.ACTIVE, artifact_uri="artifact_location",
                      metrics=[ElasticMetric(key="metric1", value=1, timestamp=1, step=1)],
                      params=[ElasticParam(key="param1", value="val1")],
                      tags=[ElasticTag(key="tag1", value="val1")],)

mock_elastic_param = ElasticParam(key="param2", value="val2")
mock_param = Param(key="param2", value="val2")

mock_elastic_tag = ElasticTag(key="tag2", value="val2")
mock_tag = RunTag(key="tag2", value="val2")


@mock.patch('mlflow_elasticsearchstore.models.ElasticExperiment')
def test_create_experiment(elastic_experiment_mock):
    connections.create_connection = MagicMock()
    store = ElasticsearchStore("user", "password", "host", "port")
    connections.create_connection.assert_called_with(
        hosts=["user:password@host:port"])

    elastic_experiment_mock.return_value = mock_experiment
    experiment_mock = elastic_experiment_mock.return_value
    experiment_mock.save = MagicMock()
    store.create_experiment("name", "artifact_location")
    elastic_experiment_mock.assert_called_once_with(name="name",
                                                    lifecycle_stage=LifecycleStage.ACTIVE,
                                                    artifact_location="artifact_location")
    experiment_mock.save.assert_called_once_with()
    # experiment_mock = elastic_experiment_mock.return_value
    # experiment_mock.save = MagicMock()
    # experiment_mock.save.assert_called_once_with()
    # assert experiment_mock.meta.id == real_experiment_id


def test_get_experiment():
    connections.create_connection = MagicMock()
    store = ElasticsearchStore("user", "password", "host", "port")
    connections.create_connection.assert_called_with(
        hosts=["user:password@host:port"])

    ElasticExperiment.get = MagicMock(return_value=mock_experiment)
    real_experiment = store.get_experiment("1")
    ElasticExperiment.get.assert_called_once_with(id="1")
    experiment_mock = ElasticExperiment.get.return_value
    assert experiment_mock.to_mlflow_entity().__dict__ == real_experiment.__dict__


def test__get_run():
    connections.create_connection = MagicMock()
    store = ElasticsearchStore("user", "password", "host", "port")
    connections.create_connection.assert_called_with(
        hosts=["user:password@host:port"])

    ElasticRun.get = MagicMock(return_value=mock_run)
    real_run = store._get_run("1")
    ElasticRun.get.assert_called_once_with(id="1")
    run_mock = ElasticRun.get.return_value
    assert run_mock.__dict__ == real_run.__dict__


def test_get_run():
    connections.create_connection = MagicMock()
    store = ElasticsearchStore("user", "password", "host", "port")
    connections.create_connection.assert_called_with(
        hosts=["user:password@host:port"])

    ElasticRun.get = MagicMock(return_value=mock_run)
    real_run = store.get_run("1")
    ElasticRun.get.assert_called_once_with(id="1")
    run_mock = ElasticRun.get.return_value
    assert run_mock.to_mlflow_entity().__dict__ == real_run.__dict__


def test_log_param():
    connections.create_connection = MagicMock()
    store = ElasticsearchStore("user", "password", "host", "port")
    connections.create_connection.assert_called_with(
        hosts=["user:password@host:port"])

    ElasticRun.get = MagicMock(return_value=mock_run)

    run_mock = ElasticRun.get.return_value
    # run_mock.params.append(mock_elastic_param)
    run_mock.save = MagicMock()

    store.log_param("1", mock_param)
    ElasticRun.get.assert_called_once_with(id="1")
    run_mock.save.assert_called_once_with()


def test_set_tag():
    connections.create_connection = MagicMock()
    store = ElasticsearchStore("user", "password", "host", "port")
    connections.create_connection.assert_called_with(
        hosts=["user:password@host:port"])

    ElasticRun.get = MagicMock(return_value=mock_run)

    run_mock = ElasticRun.get.return_value
    # run_mock.params.append(mock_elastic_tag)
    run_mock.save = MagicMock()

    store.set_tag("1", mock_tag)
    ElasticRun.get.assert_called_once_with(id="1")
    run_mock.save.assert_called_once_with()
