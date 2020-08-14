import pytest
import math
from elasticsearch.exceptions import NotFoundError

from mlflow.entities import (Experiment, ExperimentTag, Run, RunInfo, RunData, Columns,
                             Metric, Param, RunTag, ViewType, LifecycleStage, RunStatus)
from mlflow.exceptions import MlflowException

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import (ElasticExperiment, ElasticRun, ElasticMetric,
                                              ElasticParam, ElasticTag,
                                              ElasticLatestMetric, ElasticExperimentTag)

pytestmark = pytest.mark.integration


@pytest.mark.usefixtures('init_store')
def test_list_experiments(init_store):
    expected_experiment0 = Experiment(experiment_id="hTb553MBNoOYfhXjnnQh", name="exp0",
                                      lifecycle_stage="active", artifact_location="artifact_path",
                                      tags=[])

    expected_experiment1 = Experiment(experiment_id="hjb553MBNoOYfhXjp3Tn", name="exp1",
                                      lifecycle_stage="active", artifact_location="artifact_path",
                                      tags=[])

    expected_experiment2 = Experiment(experiment_id="hzb553MBNoOYfhXjsXRa", name="exp2",
                                      lifecycle_stage="active", artifact_location="artifact_path",
                                      tags=[])
    experiments = init_store.list_experiments(view_type=ViewType.ACTIVE_ONLY)
    assert experiments[0].__dict__ == expected_experiment0.__dict__
    assert experiments[1].__dict__ == expected_experiment1.__dict__
    assert experiments[2].__dict__ == expected_experiment2.__dict__


@pytest.mark.usefixtures('init_store')
def test_get_experiment_with_fake_id(init_store):
    with pytest.raises(NotFoundError) as excinfo:
        init_store.get_experiment(experiment_id="fake_id")
        assert "404" in excinfo


@pytest.mark.usefixtures('init_store')
def test_get_experiment(init_store):
    expected_experiment = Experiment(experiment_id="hjb553MBNoOYfhXjp3Tn", name="exp1",
                                     lifecycle_stage="active", artifact_location="artifact_path",
                                     tags=[])
    experiment = init_store.get_experiment(experiment_id=expected_experiment.experiment_id)
    assert experiment.__dict__ == expected_experiment.__dict__


@pytest.mark.usefixtures('init_store')
def test_create_experiment(init_store):
    exp_id = init_store.create_experiment(name="new_exp", artifact_location="artifact_location")
    new_exp = init_store.get_experiment(exp_id)
    assert new_exp.name == "new_exp"
    assert new_exp.artifact_location == "artifact_location"


@pytest.mark.usefixtures('init_store')
def test_create_experiment_with_no_name(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.create_experiment(name="", artifact_location="artifact_location")
        assert 'Invalid experiment name' in str(excinfo.value)


def test_create_experiment_with_existing_name(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.create_experiment(name="exp0", artifact_location="artifact_location")
        assert 'This experiment name already exists' in str(excinfo.value)


@pytest.mark.usefixtures('init_store')
def test_create_experiment_with_name_equal_None(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.create_experiment(name=None, artifact_location="artifact_location")
        assert 'Invalid experiment name' in str(excinfo.value)


@pytest.mark.usefixtures('init_store')
def test_restore_experiment_of_active_experiment(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.restore_experiment("hzb553MBNoOYfhXjsXRa")
        assert "Cannot restore an active experiment." in str(excinfo.value)


@pytest.mark.usefixtures('init_store')
def test_delete_experiment(init_store):
    init_store.delete_experiment("hzb553MBNoOYfhXjsXRa")
    deleted_exp = init_store.get_experiment("hzb553MBNoOYfhXjsXRa")
    assert deleted_exp.lifecycle_stage == LifecycleStage.DELETED


@pytest.mark.usefixtures('init_store')
def test_delete_experiment_of_deleted_experiment(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.delete_experiment("hzb553MBNoOYfhXjsXRa")
        assert "Cannot delete an already deleted experiment." in str(excinfo.value)


@pytest.mark.usefixtures('init_store')
def test_rename_experiment_of_deleted_experiment(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.rename_experiment("hzb553MBNoOYfhXjsXRa", "exp2renamed")
        assert "Cannot rename a non-active experiment." in str(excinfo.value)


@pytest.mark.usefixtures('init_store')
def test_restore_experiment(init_store):
    init_store.restore_experiment("hzb553MBNoOYfhXjsXRa")
    restored_exp = init_store.get_experiment("hzb553MBNoOYfhXjsXRa")
    assert restored_exp.lifecycle_stage == LifecycleStage.ACTIVE


@pytest.mark.usefixtures('init_store')
def test_rename_experiment(init_store):
    init_store.rename_experiment("hzb553MBNoOYfhXjsXRa", "exp2renamed")
    renamed_exp = init_store.get_experiment("hzb553MBNoOYfhXjsXRa")
    assert renamed_exp.name == "exp2renamed"


@pytest.mark.usefixtures('init_store')
def test_get_run(init_store):
    expected_run_info = RunInfo(run_uuid="7b2e71956f3d4c08b042624a8d83700d",
                                experiment_id="hTb553MBNoOYfhXjnnQh",
                                user_id="1",
                                status="RUNNING",
                                start_time=1597324762662,
                                end_time=None,
                                lifecycle_stage="active",
                                artifact_uri="artifact_path/7b2e71956f3d4c08b042624a8d83700d"
                                "/artifacts",
                                run_id="7b2e71956f3d4c08b042624a8d83700d")

    expected_metrics = [Metric(key="metric0", value=15.0, timestamp=1597324762700, step=0),
                        Metric(key="metric0", value=7.0, timestamp=1597324762742, step=1),
                        Metric(key="metric0", value=20.0, timestamp=1597324762778, step=2),
                        Metric(key="metric1", value=20.0, timestamp=1597324762815, step=0),
                        Metric(key="metric1", value=0.0, timestamp=1597324762847, step=1),
                        Metric(key="metric1", value=7.0, timestamp=1597324762890, step=2)]

    expected_params = [Param(key="param0", value="val2"),
                       Param(key="param1", value="Val1"),
                       Param(key="param2", value="Val1"),
                       Param(key="param3", value="valeur4")]

    expected_tags = [RunTag(key="tag0", value="val2"),
                     RunTag(key="tag1", value="test3"),
                     RunTag(key="tag2", value="val2"),
                     RunTag(key="tag3", value="test3")]

    expected_run_data = RunData(metrics=expected_metrics,
                                params=expected_params, tags=expected_tags)

    run = init_store.get_run(expected_run_info._run_id)
    assert run._info == expected_run_info
    for i, metric in enumerate(run._data._metric_objs):
        assert metric.__dict__ == expected_run_data._metric_objs[i].__dict__
    assert run._data._params == expected_run_data._params
    assert run._data._tags == expected_run_data._tags


@pytest.mark.usefixtures('init_store')
def test_create_run(init_store):
    run = init_store.create_run("hzb553MBNoOYfhXjsXRa", "2", 10, [RunTag(key="tag1", value="val1")])
    actual_run = init_store.get_run(run._info._run_id)
    assert actual_run._info == run._info
    assert actual_run._data.__dict__ == run._data.__dict__


@pytest.mark.usefixtures('init_store')
def test_update_run_info(init_store):
    run_info = init_store.update_run_info(
        "d57a45f3763e4827b7c03f03d60dbbe1", RunStatus.FINISHED, 20)
    actual_run = init_store.get_run("d57a45f3763e4827b7c03f03d60dbbe1")
    assert run_info.status == actual_run._info.status
    assert run_info.end_time == actual_run._info.end_time


@pytest.mark.usefixtures('init_store')
def test_delete_run(init_store):
    init_store.delete_run("d57a45f3763e4827b7c03f03d60dbbe1")
    run_deleted = init_store.get_run("d57a45f3763e4827b7c03f03d60dbbe1")
    assert run_deleted._info._lifecycle_stage == LifecycleStage.DELETED


@pytest.mark.usefixtures('init_store')
def test_delete_run_of_deleted_run(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.delete_run("d57a45f3763e4827b7c03f03d60dbbe1")
        assert "must be in the 'active' state" in str(excinfo.value)


@pytest.mark.usefixtures('init_store')
def test_update_run_info_of_deleted_run(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.update_run_info("d57a45f3763e4827b7c03f03d60dbbe1", RunStatus.FINISHED, 20)
        assert "must be in the 'active' state" in str(excinfo.value)


@pytest.mark.usefixtures('init_store')
def test_restore_run(init_store):
    init_store.restore_run("d57a45f3763e4827b7c03f03d60dbbe1")
    run_restored = init_store.get_run("d57a45f3763e4827b7c03f03d60dbbe1")
    assert run_restored._info._lifecycle_stage == LifecycleStage.ACTIVE


@pytest.mark.usefixtures('init_store')
def test_restore_run_of_active_run(init_store):
    with pytest.raises(MlflowException) as excinfo:
        init_store.restore_run("d57a45f3763e4827b7c03f03d60dbbe1")
        assert "must be in the 'deleted' state" in str(excinfo.value)


@pytest.mark.usefixtures('init_store')
def test_log_metric(init_store):
    new_metric = Metric(key="new_metric", value=7.0, timestamp=10, step=0)
    init_store.log_metric("7b2e71956f3d4c08b042624a8d83700d", new_metric)
    actual_run = init_store._get_run("7b2e71956f3d4c08b042624a8d83700d")
    assert ElasticMetric(key="new_metric", value=7.0, timestamp=10,
                         step=0, is_nan=False) in actual_run.metrics
    assert ElasticLatestMetric(key="new_metric", value=7.0, timestamp=10,
                               step=0, is_nan=False) in actual_run.latest_metrics


@pytest.mark.usefixtures('init_store')
def test_log_metric_with_nan_value(init_store):
    new_metric = Metric(key="nan_metric", value=math.nan, timestamp=10, step=0)
    init_store.log_metric("7b2e71956f3d4c08b042624a8d83700d", new_metric)
    actual_run = init_store._get_run("7b2e71956f3d4c08b042624a8d83700d")
    assert ElasticMetric(key="nan_metric", value=0, timestamp=10,
                         step=0, is_nan=True) in actual_run.metrics
    assert ElasticLatestMetric(key="nan_metric", value=0, timestamp=10,
                               step=0, is_nan=True) in actual_run.latest_metrics


@pytest.mark.usefixtures('init_store')
def test_log_metric_with_inf_value(init_store):
    new_metric = Metric(key="inf_metric", value=1.7976931348623157e309, timestamp=10, step=0)
    init_store.log_metric("7b2e71956f3d4c08b042624a8d83700d", new_metric)
    actual_run = init_store._get_run("7b2e71956f3d4c08b042624a8d83700d")
    assert ElasticMetric(key="inf_metric", value=1.7976931348623157e308,
                         timestamp=10, step=0, is_nan=False) in actual_run.metrics
    assert ElasticLatestMetric(key="inf_metric", value=1.7976931348623157e308,
                               timestamp=10, step=0, is_nan=False) in actual_run.latest_metrics


@pytest.mark.usefixtures('init_store')
def test_log_metric_with_negative_inf_value(init_store):
    new_metric = Metric(key="negative_inf_metric",
                        value=-1.7976931348623157e309, timestamp=10, step=0)
    init_store.log_metric("7b2e71956f3d4c08b042624a8d83700d", new_metric)
    actual_run = init_store._get_run("7b2e71956f3d4c08b042624a8d83700d")
    assert ElasticMetric(key="negative_inf_metric",
                         value=-1.7976931348623157e308, timestamp=10,
                         step=0, is_nan=False) in actual_run.metrics
    assert ElasticLatestMetric(key="negative_inf_metric",
                               value=-1.7976931348623157e308, timestamp=10,
                               step=0, is_nan=False) in actual_run.latest_metrics


@pytest.mark.usefixtures('init_store')
def test_log_metric_with_existing_key(init_store):
    new_metric = Metric(key="new_metric", value=-10, timestamp=20, step=1)
    init_store.log_metric("7b2e71956f3d4c08b042624a8d83700d", new_metric)
    actual_run = init_store._get_run("7b2e71956f3d4c08b042624a8d83700d")
    assert ElasticMetric(key="new_metric", value=-10,
                         timestamp=20, step=1, is_nan=False) in actual_run.metrics
    assert ElasticLatestMetric(key="new_metric", value=-10,
                               timestamp=20, step=1, is_nan=False) in actual_run.latest_metrics


@pytest.mark.usefixtures('init_store')
def test_log_param(init_store):
    new_param = Param(key="new_param", value="new_value")
    init_store.log_param("7b2e71956f3d4c08b042624a8d83700d", new_param)
    actual_run = init_store._get_run("7b2e71956f3d4c08b042624a8d83700d")
    assert ElasticParam(key="new_param", value="new_value") in actual_run.params


@pytest.mark.usefixtures('init_store')
def test_set_tag(init_store):
    new_tag = RunTag(key="new_tag", value="new_value")
    init_store.set_tag("7b2e71956f3d4c08b042624a8d83700d", new_tag)
    actual_run = init_store._get_run("7b2e71956f3d4c08b042624a8d83700d")
    assert ElasticTag(key="new_tag", value="new_value") in actual_run.tags


@pytest.mark.usefixtures('init_store')
def test_experiment_set_tag(init_store):
    new_experiment_tag = ExperimentTag(key="new_tag", value="new_value")
    expected_elastic_tags = [ElasticExperimentTag(key="new_tag", value="new_value")]
    init_store.set_experiment_tag("hTb553MBNoOYfhXjnnQh", new_experiment_tag)
    actual_experiment = init_store._get_experiment("hTb553MBNoOYfhXjnnQh")
    assert actual_experiment.tags == expected_elastic_tags


@pytest.mark.usefixtures('init_store')
def test_get_metric_history(init_store):
    expected_metric_history = [Metric(key="metric0", value=15.0, timestamp=1597324762700, step=0),
                               Metric(key="metric0", value=7.0, timestamp=1597324762742, step=1),
                               Metric(key="metric0", value=20.0, timestamp=1597324762778, step=2)]
    actual_metric_history = init_store.get_metric_history(
        "7b2e71956f3d4c08b042624a8d83700d", "metric0")
    for i, metric in enumerate(actual_metric_history):
        assert metric.__dict__ == expected_metric_history[i].__dict__


@pytest.mark.usefixtures('init_store')
def test_get_metric_history_with_fake_key(init_store):
    expected_metric_history = []
    actual_metric_history = init_store.get_metric_history(
        "7b2e71956f3d4c08b042624a8d83700d", "fake_key")
    assert actual_metric_history == expected_metric_history


@pytest.mark.usefixtures('init_store')
def test_get_metric_history_with_fake_run_id(init_store):
    expected_metric_history = []
    actual_metric_history = init_store.get_metric_history(
        "fake_run_id", "metric0")
    assert actual_metric_history == expected_metric_history


@pytest.mark.usefixtures('init_store')
def test_list_all_columns_all(init_store):
    expected_columns = Columns(metrics=["metric0", "metric1", "metric7"],
                               params=["param0", "param1", "param2", "param3", "param7"],
                               tags=["tag0", "tag1", "tag2", "tag3", "tag7"])
    actual_columns = init_store.list_all_columns("hjb553MBNoOYfhXjp3Tn", ViewType.ALL)
    assert expected_columns.__dict__ == actual_columns.__dict__


@pytest.mark.usefixtures('init_store')
def test_list_all_columns_active(init_store):
    expected_columns = Columns(metrics=["metric0", "metric1"],
                               params=["param0", "param1", "param2", "param3"],
                               tags=["tag0", "tag1", "tag2", "tag3"])
    actual_columns = init_store.list_all_columns("hjb553MBNoOYfhXjp3Tn", ViewType.ACTIVE_ONLY)
    assert expected_columns.__dict__ == actual_columns.__dict__


@pytest.mark.usefixtures('init_store')
def test_list_all_columns_deleted(init_store):
    expected_columns = Columns(metrics=["metric7"],
                               params=["param7"],
                               tags=["tag7"])
    actual_columns = init_store.list_all_columns("hjb553MBNoOYfhXjp3Tn", ViewType.DELETED_ONLY)
    print(actual_columns)
    print(expected_columns)
    assert expected_columns.__dict__ == actual_columns.__dict__


@pytest.mark.usefixtures('init_store')
def test_list_all_columns_with_fake_experiment_id(init_store):
    expected_columns = Columns(metrics=[],
                               params=[],
                               tags=[])
    actual_columns = init_store.list_all_columns("fake_id", ViewType.ALL)
    assert expected_columns.__dict__ == actual_columns.__dict__
