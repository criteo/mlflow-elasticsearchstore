import pytest

from mlflow.entities import (Experiment, Run, RunInfo, RunData,
                             Metric, Param, RunTag, ViewType, LifecycleStage)

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore


actual_experiment0 = Experiment(experiment_id="hTb553MBNoOYfhXjnnQh", name="exp0",
                                lifecycle_stage="active", artifact_location="artifact_path",
                                tags=[])

actual_experiment1 = Experiment(experiment_id="hjb553MBNoOYfhXjp3Tn", name="exp1",
                                lifecycle_stage="active", artifact_location="artifact_path",
                                tags=[])

actual_experiment2 = Experiment(experiment_id="hzb553MBNoOYfhXjsXRa", name="exp2",
                                lifecycle_stage="active", artifact_location="artifact_path",
                                tags=[])

actual_run_info = RunInfo(run_uuid="7b2e71956f3d4c08b042624a8d83700d",
                          experiment_id="hTb553MBNoOYfhXjnnQh",
                          user_id="1",
                          status="RUNNING",
                          start_time=1597324762662,
                          end_time=None,
                          lifecycle_stage="active",
                          artifact_uri="artifact_path/7b2e71956f3d4c08b042624a8d83700d/artifacts",
                          run_id="7b2e71956f3d4c08b042624a8d83700d")

actual_metrics = [Metric(key="metric0", value=15.0, timestamp=1597324762700, step=0),
                  Metric(key="metric0", value=7.0, timestamp=1597324762742, step=1),
                  Metric(key="metric0", value=20.0, timestamp=1597324762778, step=2),
                  Metric(key="metric1", value=20.0, timestamp=1597324762815, step=0),
                  Metric(key="metric1", value=0.0, timestamp=1597324762847, step=1),
                  Metric(key="metric1", value=7.0, timestamp=1597324762890, step=2)]

actual_params = [Param(key="param0", value="val2"),
                 Param(key="param1", value="Val1"),
                 Param(key="param2", value="Val1"),
                 Param(key="param3", value="valeur4")]

actual_tags = [RunTag(key="tag0", value="val2"),
               RunTag(key="tag1", value="test3"),
               RunTag(key="tag2", value="val2"),
               RunTag(key="tag3", value="test3")]

actual_run_data = RunData(metrics=actual_metrics, params=actual_params, tags=actual_tags)

actual_run = Run(run_info=actual_run_info, run_data=actual_run_data)


@pytest.mark.integration
@pytest.mark.usefixtures('init_store')
def test_get_experiment(init_store):
    experiment = init_store.get_experiment(experiment_id=actual_experiment1.experiment_id)
    assert experiment.__dict__ == actual_experiment1.__dict__


@pytest.mark.integration
@pytest.mark.usefixtures('init_store')
def test_list_experiments(init_store):
    experiments = init_store.list_experiments(view_type=ViewType.ACTIVE_ONLY)
    assert experiments[0].__dict__ == actual_experiment0.__dict__
    assert experiments[1].__dict__ == actual_experiment1.__dict__
    assert experiments[2].__dict__ == actual_experiment2.__dict__


@pytest.mark.integration
@pytest.mark.usefixtures('init_store')
def test_get_run(init_store):
    run = init_store.get_run(actual_run._info._run_id)
    assert run._info.__dict__ == actual_run._info.__dict__
    for i, metric in enumerate(run._data._metric_objs):
        assert metric.__dict__ == actual_run._data._metric_objs[i].__dict__
    assert run._data._params == actual_run._data._params
    assert run._data._tags == actual_run._data._tags


@pytest.mark.integration
@pytest.mark.usefixtures('init_store')
def test_create_experiment(init_store):
    exp_id = init_store.create_experiment(name="new_exp", artifact_location="artifact_location")
    new_exp = init_store.get_experiment(exp_id)
    assert new_exp.name == "new_exp"
    assert new_exp.artifact_location == "artifact_location"


@pytest.mark.integration
@pytest.mark.usefixtures('init_store')
def test_delete_experiment(init_store):
    init_store.delete_experiment("hzb553MBNoOYfhXjsXRa")
    deleted_exp = init_store.get_experiment("hzb553MBNoOYfhXjsXRa")
    assert deleted_exp.lifecycle_stage == LifecycleStage.DELETED


@pytest.mark.integration
@pytest.mark.usefixtures('init_store')
def test_restore_experiment(init_store):
    init_store.restore_experiment("hzb553MBNoOYfhXjsXRa")
    restored_exp = init_store.get_experiment("hzb553MBNoOYfhXjsXRa")
    assert restored_exp.lifecycle_stage == LifecycleStage.ACTIVE


@pytest.mark.integration
@pytest.mark.usefixtures('init_store')
def test_rename_experiment(init_store):
    init_store.rename_experiment("hzb553MBNoOYfhXjsXRa", "exp2renamed")
    renamed_exp = init_store.get_experiment("hzb553MBNoOYfhXjsXRa")
    assert renamed_exp.name == "exp2renamed"
