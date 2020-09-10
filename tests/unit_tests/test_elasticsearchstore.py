import pytest
import mock
from types import SimpleNamespace
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch_dsl import Search, Q

from mlflow.entities import (RunTag, Metric, Param, RunStatus,
                             LifecycleStage, ViewType, ExperimentTag)

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import ExperimentIndex, RunIndex
from mlflow_elasticsearchstore.models import (ElasticExperiment, ElasticRun, ElasticExperimentTag,
                                              ElasticLatestMetric, ElasticMetric,
                                              ElasticParam, ElasticTag)

experiment_response = {'_index': 'mlflow-experiments', '_type': '_doc', '_id': '1',
                       '_version': 1, '_seq_no': 0, '_primary_term': 1, 'found': True,
                       '_source': {'name': 'name',
                                   'lifecycle_stage': 'active',
                                   'artifact_location': 'artifact_location'}}
experiment_deleted_response = {'_index': 'mlflow-experiments', '_type': '_doc', '_id': '1',
                               '_version': 1, '_seq_no': 0, '_primary_term': 1, 'found': True,
                               '_source': {'name': 'name',
                                           'lifecycle_stage': 'deleted',
                                           'artifact_location': 'artifact_location'}}
deleted_experiment = ElasticExperiment(meta={'id': "1"}, name="name",
                                       lifecycle_stage=LifecycleStage.DELETED,
                                       artifact_location="artifact_location")

experiment_tag = ExperimentTag(key="tag1", value="val1")
elastic_experiment_tag = ElasticExperimentTag(key="tag1", value="val1")
experiment = ElasticExperiment(meta={'id': "1"}, name="name",
                               lifecycle_stage=LifecycleStage.ACTIVE,
                               artifact_location="artifact_location")

run = ElasticRun(meta={'id': "1"},
                 experiment_id="experiment_id", user_id="user_id",
                 status=RunStatus.to_string(RunStatus.RUNNING),
                 start_time=1, end_time=None,
                 lifecycle_stage=LifecycleStage.ACTIVE, artifact_uri="artifact_location",
                 metrics=[ElasticMetric(key="metric1", value=1,
                                        timestamp=1, step=1, is_nan=False)],
                 latest_metrics=[ElasticLatestMetric(
                     key="metric1", value=1, timestamp=1, step=1, is_nan=False)],
                 params=[ElasticParam(key="param1", value="val1")],
                 tags=[ElasticTag(key="tag1", value="val1")])

deleted_run = ElasticRun(meta={'id': "1"},
                         experiment_id="experiment_id", user_id="user_id",
                         status=RunStatus.to_string(RunStatus.RUNNING),
                         start_time=1, end_time=None,
                         lifecycle_stage=LifecycleStage.DELETED,
                         artifact_uri="artifact_location",
                         metrics=[ElasticMetric(key="metric1", value=1,
                                                timestamp=1, step=1, is_nan=False)],
                         latest_metrics=[ElasticLatestMetric(
                             key="metric1", value=1, timestamp=1, step=1, is_nan=False)],
                         params=[ElasticParam(key="param1", value="val1")],
                         tags=[ElasticTag(key="tag1", value="val1")])

elastic_metric = ElasticMetric(key="metric2", value=2, timestamp=1, step=1, is_nan=False)

metric = Metric(key="metric2", value=2, timestamp=1, step=1)

elastic_param = ElasticParam(key="param2", value="val2")
param = Param(key="param2", value="val2")

elastic_tag = ElasticTag(key="tag2", value="val2")
tag = RunTag(key="tag2", value="val2")


@mock.patch('elasticsearch.Elasticsearch.search')
@pytest.mark.usefixtures('create_store')
def test_list_experiments(search_mock, create_store):
    response = {'hits':
                {'hits': [{'_index': 'mlflow-experiments', '_type': '_doc',
                           '_id': '1', '_score': 0.0,
                           '_source': {'name': 'name',
                                       'lifecycle_stage': 'active',
                                       'artifact_location': 'artifact_location'}}]}}
    search_mock.return_value = response
    real_experiments = create_store.list_experiments()
    body = {
        "query": {
            "bool": {
                "filter": [
                    {"terms": {"lifecycle_stage": ['active']}}
                ]
            }
        }
    }
    search_mock.assert_called_once_with(index=ExperimentIndex.name, body=body)
    mock_experiments = [create_store._hit_to_mlflow_experiment(e) for e in response["hits"]["hits"]]
    assert real_experiments[0].__dict__ == mock_experiments[0].__dict__


@mock.patch('elasticsearch.Elasticsearch.get')
@pytest.mark.usefixtures('create_store')
def test_get_experiment(elastic_get_mock, create_store):
    elastic_get_mock.return_value = experiment_response
    real_experiment = create_store.get_experiment("1")
    elastic_get_mock.assert_called_once_with(index=ExperimentIndex.name, id="1")
    assert create_store._hit_to_mlflow_experiment(
        experiment_response).__dict__ == real_experiment.__dict__


@mock.patch('elasticsearch.Elasticsearch.get')
@mock.patch('elasticsearch.Elasticsearch.update')
@pytest.mark.usefixtures('create_store')
def test_delete_experiment(elastic_update_mock, elastic_get_mock, create_store):
    elastic_get_mock.return_value = experiment_response
    create_store.delete_experiment("1")
    elastic_get_mock.assert_called_once_with(index=ExperimentIndex.name, id="1")
    elastic_update_mock.assert_called_once_with(
        index=ExperimentIndex.name, id="1",
        body={"doc": {"lifecycle_stage": LifecycleStage.DELETED}},
        refresh=True)


@mock.patch('elasticsearch.Elasticsearch.get')
@mock.patch('elasticsearch.Elasticsearch.update')
@pytest.mark.usefixtures('create_store')
def test_restore_experiment(elastic_update_mock, elastic_get_mock, create_store):
    elastic_get_mock.return_value = experiment_deleted_response
    create_store.restore_experiment("1")
    elastic_get_mock.assert_called_once_with(index=ExperimentIndex.name, id="1")
    elastic_update_mock.assert_called_once_with(
        index=ExperimentIndex.name, id="1",
        body={"doc": {"lifecycle_stage": LifecycleStage.ACTIVE}},
        refresh=True)


@mock.patch('elasticsearch.Elasticsearch.get')
@mock.patch('elasticsearch.Elasticsearch.update')
@pytest.mark.usefixtures('create_store')
def test_rename_experiment(elastic_update_mock, elastic_get_mock, create_store):
    elastic_get_mock.return_value = experiment_response
    create_store.rename_experiment("1", "new_name")
    elastic_get_mock.assert_called_once_with(index=ExperimentIndex.name, id="1")
    elastic_update_mock.assert_called_once_with(
        index=ExperimentIndex.name, id="1",
        body={"doc": {"name": "new_name"}},
        refresh=True)


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.save')
@mock.patch('mlflow_elasticsearchstore.models.ElasticExperiment.get')
@mock.patch('uuid.uuid4')
@pytest.mark.usefixtures('create_store')
def test_create_run(uuid_mock, elastic_experiment_get_mock,
                    elastic_run_save_mock, create_store):
    uuid_mock.return_value = SimpleNamespace(hex='run_id')
    elastic_experiment_get_mock.return_value = experiment
    real_run = create_store.create_run(
        experiment_id="1", user_id="user_id", start_time=1, tags=[])
    uuid_mock.assert_called_once_with()
    elastic_experiment_get_mock.assert_called_once_with(id="1")
    elastic_run_save_mock.assert_called_once_with()
    assert real_run._info.experiment_id == "1"
    assert real_run._info.user_id == "user_id"
    assert real_run._info.start_time == 1
    assert real_run._data.tags == {}
    assert real_run._info.run_id == "run_id"


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_delete_run(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run.update = mock.MagicMock()
    create_store.delete_run("1")
    elastic_run_get_mock.assert_called_once_with(id="1")
    run.update.assert_called_once_with(lifecycle_stage=LifecycleStage.DELETED)


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_restore_run(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = deleted_run
    deleted_run.update = mock.MagicMock()
    create_store.restore_run("1")
    elastic_run_get_mock.assert_called_once_with(id="1")
    deleted_run.update.assert_called_once_with(lifecycle_stage=LifecycleStage.ACTIVE)


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_update_run_info(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run.update = mock.MagicMock()
    create_store.update_run_info("1", RunStatus.FINISHED, 2)
    elastic_run_get_mock.assert_called_once_with(id="1")
    run.update.assert_called_once_with(
        status=RunStatus.to_string(RunStatus.FINISHED), end_time=2)


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test__get_run(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    real_run = create_store._get_run("1")
    ElasticRun.get.assert_called_once_with(id="1")
    assert run == real_run


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_get_run(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    real_run = create_store.get_run("1")
    ElasticRun.get.assert_called_once_with(id="1")
    assert run.to_mlflow_entity()._info == real_run._info
    assert run.to_mlflow_entity()._data._metrics == real_run._data._metrics
    assert run.to_mlflow_entity()._data._params == real_run._data._params
    assert run.to_mlflow_entity()._data._tags == real_run._data._tags


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@mock.patch('mlflow_elasticsearchstore.elasticsearch_store.ElasticsearchStore.'
            '_update_latest_metric_if_necessary')
@pytest.mark.usefixtures('create_store')
def test_log_metric(_update_latest_metric_if_necessary_mock, elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run.metrics = mock.MagicMock()
    run.metrics.append = mock.MagicMock()
    run.save = mock.MagicMock()
    create_store.log_metric("1", metric)
    elastic_run_get_mock.assert_called_once_with(id="1")
    _update_latest_metric_if_necessary_mock.assert_called_once_with(elastic_metric, run)
    run.metrics.append.assert_called_once_with(elastic_metric)
    run.save.assert_called_once_with()


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_log_param(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run.params = mock.MagicMock()
    run.params.append = mock.MagicMock()
    run.save = mock.MagicMock()
    create_store.log_param("1", param)
    elastic_run_get_mock.assert_called_once_with(id="1")
    run.params.append.assert_called_once_with(elastic_param)
    run.save.assert_called_once_with()


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticExperiment.get')
@pytest.mark.usefixtures('create_store')
def test_set_experiment_tag(elastic_experiment_get_mock, create_store):
    elastic_experiment_get_mock.return_value = experiment
    experiment.tags = mock.MagicMock()
    experiment.tags.append = mock.MagicMock()
    experiment.save = mock.MagicMock()
    create_store.set_experiment_tag("1", experiment_tag)
    elastic_experiment_get_mock.assert_called_once_with(id="1")
    experiment.tags.append.assert_called_once_with(elastic_experiment_tag)
    experiment.save.assert_called_once_with()


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_set_tag(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run.tags = mock.MagicMock()
    run.tags.append = mock.MagicMock()
    run.save = mock.MagicMock()
    create_store.set_tag("1", tag)
    elastic_run_get_mock.assert_called_once_with(id="1")
    run.tags.append.assert_called_once_with(elastic_tag)
    run.save.assert_called_once_with()


@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize("test_elastic_metric,test_elastic_latest_metrics",
                         [(ElasticMetric(key="metric1", value=2, timestamp=1,
                                         step=2, is_nan=False),
                           [ElasticLatestMetric(key="metric1", value=2, timestamp=1,
                                                step=2, is_nan=False)]),

                          (ElasticMetric(key="metric2", value=2, timestamp=1,
                                         step=1, is_nan=False),
                           [ElasticLatestMetric(key="metric1", value=2, timestamp=1,
                                                step=2, is_nan=False),
                            ElasticLatestMetric(key="metric2", value=2, timestamp=1,
                                                step=1, is_nan=False)])])
@pytest.mark.usefixtures('create_store')
def test__update_latest_metric_if_necessary(test_elastic_metric, test_elastic_latest_metrics,
                                            create_store):
    create_store._update_latest_metric_if_necessary(test_elastic_metric, run)
    assert run.latest_metrics == test_elastic_latest_metrics


@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize("test_parsed_filter,test_query,test_type",
                         [({'type': 'parameter', 'key': 'param0',
                             'comparator': 'LIKE', 'value': '%va%'},
                           Q("term", params__key="param0") &
                           Q('bool', must=[Q("wildcard", params__value="*va*")]),
                           "params"),
                          ({'type': 'parameter', 'key': 'param0',
                            'comparator': 'ILIKE', 'value': '%va%'},
                           Q("term", params__key="param0") &
                           Q('bool', must=[Q("wildcard", params__value="*va*")]),
                           "params"),
                          ({'type': 'parameter', 'key': 'param0',
                            'comparator': '=', 'value': 'va'},
                           Q("term", params__key="param0") &
                           Q('bool', must=[Q("term", params__value="va")]),
                           "params"),
                          ({'type': 'metric', 'key': 'metric0', 'comparator': '>', 'value': '1'},
                           Q("term", latest_metrics__key="metric0") &
                           Q('bool', must=[Q("range", latest_metrics__value={'gt': "1"})]),
                           "latest_metrics"),
                          ({'type': 'metric', 'key': 'metric0', 'comparator': '>=', 'value': '1'},
                           Q("term", latest_metrics__key="metric0") &
                           Q('bool', must=[Q("range", latest_metrics__value={'gte': "1"})]),
                           "latest_metrics"),
                          ({'type': 'metric', 'key': 'metric0', 'comparator': '<', 'value': '1'},
                           Q("term", latest_metrics__key="metric0") &
                           Q('bool', must=[Q("range", latest_metrics__value={'lt': "1"})]),
                           "latest_metrics"),
                          ({'type': 'metric', 'key': 'metric0', 'comparator': '<=', 'value': '1'},
                           Q("term", latest_metrics__key="metric0") &
                           Q('bool', must=[Q("range", latest_metrics__value={'lte': "1"})]),
                           "latest_metrics"),
                          ({'type': 'tag', 'key': 'tag0', 'comparator': '!=', 'value': 'val2'},
                           Q("term", tags__key="tag0") &
                           Q('bool', must_not=[Q("term", tags__value="val2")]),
                           "tags")])
@pytest.mark.usefixtures('create_store')
def test___build_elasticsearch_query(test_parsed_filter, test_query,
                                     test_type, create_store):
    actual_query = create_store._build_elasticsearch_query(
        parsed_filters=[test_parsed_filter], s=Search())
    expected_query = Search().query('nested', path=test_type, query=test_query)
    assert actual_query == expected_query


@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.usefixtures('create_store')
def test___get_orderby_clauses(create_store):
    order_by_list = ['metrics.`metric0` ASC', 'params.`param0` DESC', 'attributes.start_time ASC']
    actual_query = create_store._get_orderby_clauses(order_by_list=order_by_list, s=Search())
    sort_clauses = [{'latest_metrics.value': {'order': "asc",
                                              "nested": {"path": "latest_metrics",
                                                         "filter": {"term": {'latest_metrics.key':
                                                                             "metric0"}}}}},
                    {'params.value': {'order': "desc",
                                      "nested": {"path": "params",
                                                 "filter": {"term": {'params.key': "param0"}}}}},
                    {"start_time": {'order': "asc"}},
                    {"start_time": {'order': "desc"}},
                    {"_id": {'order': "asc"}}]
    expected_query = Search().sort(*sort_clauses)
    assert actual_query == expected_query


@pytest.mark.skip(reason="no way of currently testing this")
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun.get')
@pytest.mark.usefixtures('create_store')
def test_update_artifacts_location(elastic_run_get_mock, create_store):
    elastic_run_get_mock.return_value = run
    run.update = mock.MagicMock()
    create_store.update_artifacts_location("1", "update_artifacts_location")
    elastic_run_get_mock.assert_called_once_with(id="1")
    run.update.assert_called_once_with(artifact_uri="update_artifacts_location")
