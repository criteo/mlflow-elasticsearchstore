import pytest
import mock

from mlflow.entities import RunTag, Metric, Param, RunStatus, LifecycleStage

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import (ElasticExperiment, ElasticRun,
                                              ElasticMetric, ElasticParam, ElasticTag)

experiment = ElasticExperiment(meta={'id': "1"}, name="name",
                               lifecycle_stage=LifecycleStage.ACTIVE,
                               artifact_location="artifact_location")

run = ElasticRun(meta={'id': "1"},
                 experiment_id="experiment_id", user_id="user_id",
                 status=RunStatus.to_string(RunStatus.RUNNING),
                 start_time=1, end_time=None,
                 lifecycle_stage=LifecycleStage.ACTIVE, artifact_uri="artifact_location",
                 metrics=[ElasticMetric(key="metric1", value=1, timestamp=1, step=1)],
                 params=[ElasticParam(key="param1", value="val1")],
                 tags=[ElasticTag(key="tag1", value="val1")],)

elastic_metric = ElasticMetric(key="metric2", value=2, timestamp=1, step=1)
metric = Metric(key="metric2", value=2, timestamp=1, step=1)

elastic_param = ElasticParam(key="param2", value="val2")
param = Param(key="param2", value="val2")

elastic_tag = ElasticTag(key="tag2", value="val2")
tag = RunTag(key="tag2", value="val2")


@mock.patch('mlflow_elasticsearchstore.models.ElasticExperiment.get')
@pytest.mark.usefixtures('create_store')
def test_get_experiment(elastic_experiment_get_mock, create_store):
    elastic_experiment_get_mock.return_value = experiment
    real_experiment = create_store.get_experiment("1")
    ElasticExperiment.get.assert_called_once_with(id="1")
    experiment_mock = elastic_experiment_get_mock.return_value
    assert experiment_mock.to_mlflow_entity().__dict__ == real_experiment.__dict__


@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.save')
@mock.patch('mlflow_elasticsearchstore.models.ElasticExperiment.get')
@mock.patch('uuid.uuid4')
@pytest.mark.usefixtures('create_store')
def test_create_run(uuid_mock, elastic_experiment_get_mock,
                    elastic_run_save_mock, create_store):
    uuid_mock.hex.return_value = "run_id"
    elastic_experiment_get_mock.return_value = experiment
    real_run = create_store.create_run(experiment_id="1", user_id="user_id", start_time=1, tags=[])
    uuid_mock.assert_called_once_with()
    elastic_experiment_get_mock.assert_called_once_with(id="1")
    elastic_run_save_mock.assert_called_once_with()
    assert real_run._info.experiment_id == "1"
    assert real_run._info.user_id == "user_id"
    assert real_run._info.start_time == 1
    assert real_run._data.tags == {}


@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test__get_run(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    real_run = create_store._get_run("1")
    ElasticRun.get.assert_called_once_with(id="1")
    run_mock = elastic_run_get_mock.return_value
    assert run_mock.__dict__ == real_run.__dict__


@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_get_run(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    real_run = create_store.get_run("1")
    ElasticRun.get.assert_called_once_with(id="1")
    run_mock = elastic_run_get_mock.return_value
    assert run_mock.to_mlflow_entity()._info.__dict__ == real_run._info.__dict__
    assert run_mock.to_mlflow_entity()._data._metrics == real_run._data._metrics
    assert run_mock.to_mlflow_entity()._data._params == real_run._data._params
    assert run_mock.to_mlflow_entity()._data._tags == real_run._data._tags
    assert (run_mock.to_mlflow_entity()._data._metric_objs[0].__dict__ ==
            real_run._data._metric_objs[0].__dict__)


@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_log_metric(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run_mock = elastic_run_get_mock.return_value
    run_mock.save = mock.MagicMock()
    create_store.log_metric("1", metric)
    elastic_run_get_mock.assert_called_once_with(id="1")
    run_mock.save.assert_called_once_with()


@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_log_param(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run_mock = elastic_run_get_mock.return_value
    run_mock.save = mock.MagicMock()
    create_store.log_param("1", param)
    elastic_run_get_mock.assert_called_once_with(id="1")
    run_mock.save.assert_called_once_with()


@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_set_tag(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run_mock = elastic_run_get_mock.return_value
    run_mock.save = mock.MagicMock()
    create_store.set_tag("1", tag)
    elastic_run_get_mock.assert_called_once_with(id="1")
    run_mock.save.assert_called_once_with()
