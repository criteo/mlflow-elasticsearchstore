import uuid
import math
import re
from typing import List, Tuple, Any
from elasticsearch_dsl import Search, connections, Q
import time
from six.moves import urllib

from mlflow.store.tracking.abstract_store import AbstractStore
from mlflow.store.tracking import SEARCH_MAX_RESULTS_THRESHOLD, SEARCH_MAX_RESULTS_DEFAULT
from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE, INVALID_STATE
from mlflow.entities import (Experiment, RunTag, Metric, Param, Run, RunInfo, RunData,
                             RunStatus, ExperimentTag, LifecycleStage, ViewType, Columns)
from mlflow.exceptions import MlflowException
from mlflow.utils.uri import append_to_uri_path
from mlflow.utils.search_utils import SearchUtils

from mlflow_elasticsearchstore.models import (ElasticExperiment, ElasticRun, ElasticMetric,
                                              ElasticParam, ElasticTag,
                                              ElasticLatestMetric, ElasticExperimentTag)


class ElasticsearchStore(AbstractStore):

    ARTIFACTS_FOLDER_NAME = "artifacts"
    DEFAULT_EXPERIMENT_ID = "0"
    filter_key = {
        ">": ["range", "must"],
        ">=": ["range", "must"],
        "=": ["term", "must"],
        "!=": ["term", "must_not"],
        "<=": ["range", "must"],
        "<": ["range", "must"],
        "LIKE": ["wildcard", "must"],
        "ILIKE": ["wildcard", "must"]
    }

    def __init__(self, store_uri: str = None, artifact_uri: str = None) -> None:
        self.is_plugin = True
        connections.create_connection(hosts=[urllib.parse.urlparse(store_uri).netloc])
        ElasticExperiment.init()
        ElasticRun.init()
        super(ElasticsearchStore, self).__init__()

    def _hit_to_mlflow_experiment(self, hit: Any) -> Experiment:
        return Experiment(experiment_id=hit.meta.id, name=hit.name,
                          artifact_location=hit.artifact_location,
                          lifecycle_stage=hit.lifecycle_stage)

    def _hit_to_mlflow_run(self, hit: Any) -> Run:
        return Run(run_info=self._hit_to_mlflow_run_info(hit),
                   run_data=self._hit_to_mlflow_run_data(hit))

    def _hit_to_mlflow_run_info(self, hit: Any) -> RunInfo:
        return RunInfo(run_uuid=hit.meta.id, run_id=hit.meta.id,
                       experiment_id=str(hit.experiment_id),
                       user_id=hit.user_id,
                       status=hit.status,
                       start_time=hit.start_time,
                       end_time=hit.end_time if hasattr(hit, 'end_time') else None,
                       lifecycle_stage=hit.lifecycle_stage, artifact_uri=hit.artifact_uri)

    def _hit_to_mlflow_run_data(self, hit: Any) -> RunData:
        return RunData(metrics=[self._hit_to_mlflow_metric(m) for m in
                                (hit.latest_metrics if hasattr(hit, 'latest_metrics') else [])],
                       params=[self._hit_to_mlflow_param(p) for p in
                               (hit.params if hasattr(hit, 'params') else [])],
                       tags=[self._hit_to_mlflow_tag(t) for t in
                             (hit.tags if hasattr(hit, 'tags') else[])])

    def _hit_to_mlflow_metric(self, hit: Any) -> Metric:
        return Metric(key=hit.key, value=hit.value, timestamp=hit.timestamp,
                      step=hit.step)

    def _hit_to_mlflow_param(self, hit: Any) -> Param:
        return Param(key=hit.key, value=hit.value)

    def _hit_to_mlflow_tag(self, hit: Any) -> RunTag:
        return RunTag(key=hit.key, value=hit.value)

    def list_experiments(self, view_type: str = ViewType.ACTIVE_ONLY) -> List[Experiment]:
        stages = LifecycleStage.view_type_to_stages(view_type)
        response = Search(index="mlflow-experiments").filter("terms",
                                                             lifecycle_stage=stages).execute()
        return [self._hit_to_mlflow_experiment(e) for e in response]

    def _list_experiments_name(self) -> List[str]:
        s = Search(index="mlflow-experiments")
        s.aggs.bucket("exp_names", "terms", field="name")
        response = s.execute()
        return [name.key for name in response.aggregations.exp_names.buckets]

    def create_experiment(self, name: str, artifact_location: str = None) -> str:
        if name is None or name == '':
            raise MlflowException('Invalid experiment name', INVALID_PARAMETER_VALUE)
        existing_names = self._list_experiments_name()
        if name in existing_names:
            raise MlflowException('This experiment name already exists', INVALID_PARAMETER_VALUE)
        experiment = ElasticExperiment(name=name, lifecycle_stage=LifecycleStage.ACTIVE,
                                       artifact_location=artifact_location)
        experiment.save(refresh=True)
        return str(experiment.meta.id)

    def _get_experiment(self, experiment_id: str) -> ElasticExperiment:
        experiment = ElasticExperiment.get(id=experiment_id)
        return experiment

    def get_experiment(self, experiment_id: str) -> Experiment:
        return self._get_experiment(experiment_id).to_mlflow_entity()

    def delete_experiment(self, experiment_id: str) -> None:
        experiment = self._get_experiment(experiment_id)
        if experiment.lifecycle_stage != LifecycleStage.ACTIVE:
            raise MlflowException('Cannot delete an already deleted experiment.', INVALID_STATE)
        experiment.update(refresh=True, lifecycle_stage=LifecycleStage.DELETED)

    def restore_experiment(self, experiment_id: str) -> None:
        experiment = self._get_experiment(experiment_id)
        if experiment.lifecycle_stage != LifecycleStage.DELETED:
            raise MlflowException('Cannot restore an active experiment.', INVALID_STATE)
        experiment.update(refresh=True, lifecycle_stage=LifecycleStage.ACTIVE)

    def rename_experiment(self, experiment_id: str, new_name: str) -> None:
        experiment = self._get_experiment(experiment_id)
        if experiment.lifecycle_stage != LifecycleStage.ACTIVE:
            raise MlflowException('Cannot rename a non-active experiment.', INVALID_STATE)
        experiment.update(refresh=True, name=new_name)

    def create_run(self, experiment_id: str, user_id: str,
                   start_time: int, tags: List[RunTag]) -> Run:
        run_id = uuid.uuid4().hex
        experiment = self.get_experiment(experiment_id)
        artifact_location = append_to_uri_path(experiment.artifact_location, run_id,
                                               ElasticsearchStore.ARTIFACTS_FOLDER_NAME)

        tags_dict = {}
        for tag in tags:
            tags_dict[tag.key] = tag.value
        run_tags = [ElasticTag(key=key, value=value) for key, value in tags_dict.items()]
        run = ElasticRun(meta={'id': run_id},
                         experiment_id=experiment_id, user_id=user_id,
                         status=RunStatus.to_string(RunStatus.RUNNING),
                         start_time=start_time, end_time=None,
                         lifecycle_stage=LifecycleStage.ACTIVE, artifact_uri=artifact_location,
                         tags=run_tags)
        run.save()
        return run.to_mlflow_entity()

    def _check_run_is_active(self, run: ElasticRun) -> None:
        if run.lifecycle_stage != LifecycleStage.ACTIVE:
            raise MlflowException("The run {} must be in the 'active' state. Current state is {}."
                                  .format(run.meta.id, run.lifecycle_stage),
                                  INVALID_PARAMETER_VALUE)

    def _check_run_is_deleted(self, run: ElasticRun) -> None:
        if run.lifecycle_stage != LifecycleStage.DELETED:
            raise MlflowException("The run {} must be in the 'deleted' state. Current state is {}."
                                  .format(run.meta.id, run.lifecycle_stage),
                                  INVALID_PARAMETER_VALUE)

    def update_run_info(self, run_id: str, run_status: RunStatus, end_time: int) -> RunInfo:
        run = self._get_run(run_id)
        self._check_run_is_active(run)
        run.update(status=RunStatus.to_string(run_status), end_time=end_time)
        return run.to_mlflow_entity()._info

    def get_run(self, run_id: str) -> Run:
        run = self._get_run(run_id=run_id)
        return run.to_mlflow_entity()

    def _get_run(self, run_id: str) -> ElasticRun:
        run = ElasticRun.get(id=run_id)
        return run

    def delete_run(self, run_id: str) -> None:
        run = self._get_run(run_id)
        self._check_run_is_active(run)
        run.update(lifecycle_stage=LifecycleStage.DELETED)

    def restore_run(self, run_id: str) -> None:
        run = self._get_run(run_id)
        self._check_run_is_deleted(run)
        run.update(lifecycle_stage=LifecycleStage.ACTIVE)

    @staticmethod
    def _update_latest_metric_if_necessary(new_metric: ElasticMetric, run: ElasticRun) -> None:
        def _compare_metrics(metric_a: ElasticLatestMetric, metric_b: ElasticLatestMetric) -> bool:
            return (metric_a.step, metric_a.timestamp, metric_a.value) > \
                   (metric_b.step, metric_b.timestamp, metric_b.value)
        new_latest_metric = ElasticLatestMetric(key=new_metric.key,
                                                value=new_metric.value,
                                                timestamp=new_metric.timestamp,
                                                step=new_metric.step,
                                                is_nan=new_metric.is_nan)
        latest_metric_exist = False
        for i, latest_metric in enumerate(run.latest_metrics):
            if latest_metric.key == new_metric.key:
                latest_metric_exist = True
                if _compare_metrics(new_latest_metric, latest_metric):
                    run.latest_metrics[i] = new_latest_metric
        if not (latest_metric_exist):
            run.latest_metrics.append(new_latest_metric)

    def log_metric(self, run_id: str, metric: Metric) -> None:
        is_nan = math.isnan(metric.value)
        if is_nan:
            value = 0.
        elif math.isinf(metric.value):
            value = 1.7976931348623157e308 if metric.value > 0 else -1.7976931348623157e308
        else:
            value = metric.value
        run = self._get_run(run_id=run_id)
        new_metric = ElasticMetric(key=metric.key,
                                   value=value,
                                   timestamp=metric.timestamp,
                                   step=metric.step,
                                   is_nan=is_nan)
        self._update_latest_metric_if_necessary(new_metric, run)
        run.metrics.append(new_metric)
        run.save()

    def log_param(self, run_id: str, param: Param) -> None:
        run = self._get_run(run_id=run_id)
        new_param = ElasticParam(key=param.key,
                                 value=param.value)
        run.params.append(new_param)
        run.save()

    def set_experiment_tag(self, experiment_id: str, tag: ExperimentTag) -> None:
        experiment = self._get_experiment(experiment_id)
        new_tag = ElasticExperimentTag(key=tag.key, value=tag.value)
        experiment.tags.append(new_tag)
        experiment.save()

    def set_tag(self, run_id: str, tag: RunTag) -> None:
        run = self._get_run(run_id=run_id)
        new_tag = ElasticTag(key=tag.key,
                             value=tag.value)
        run.tags.append(new_tag)
        run.save()

    def get_metric_history(self, run_id: str, metric_key: str) -> List[Metric]:
        response = Search(index="mlflow-runs").filter("ids", values=[run_id]) \
            .filter('nested', inner_hits={"size": 100}, path="metrics",
                    query=Q('term', metrics__key=metric_key)).source(False).execute()
        return ([self._hit_to_mlflow_metric(m["_source"]) for m in
                 response["hits"]["hits"][0].inner_hits.metrics.hits.hits]
                if (len(response["hits"]["hits"]) != 0) else [])

    def list_all_columns(self, experiment_id: str, run_view_type: str) -> Columns:
        stages = LifecycleStage.view_type_to_stages(run_view_type)
        s = Search(index="mlflow-runs").filter("match", experiment_id=experiment_id) \
            .filter("terms", lifecycle_stage=stages)
        for col in ['latest_metrics', 'params', 'tags']:
            s.aggs.bucket(col, 'nested', path=col)\
                .bucket(f'{col}_keys', "terms", field=f'{col}.key')
        response = s.execute()
        metrics = [m.key for m in response.aggregations.latest_metrics.latest_metrics_keys.buckets]
        params = [p.key for p in response.aggregations.params.params_keys.buckets]
        tags = [t.key for t in response.aggregations.tags.tags_keys.buckets]
        return Columns(metrics=metrics, params=params, tags=tags)

    def _build_elasticsearch_query(self, parsed_filters: List[dict], s: Search) -> Search:
        type_dict = {"metric": "latest_metrics", "parameter": "params", "tag": "tags"}
        for search_filter in parsed_filters:
            key_type = search_filter.get('type')
            key_name = search_filter.get('key')
            value = search_filter.get('value')
            comparator = search_filter.get('comparator').upper()
            filter_ops = {
                ">": {'gt': value},
                ">=": {'gte': value},
                "=": value,
                "!=": value,
                "<=": {'lte': value},
                "<": {'lt': value}
            }
            if comparator in ["LIKE", "ILIKE"]:
                filter_ops[comparator] = f'*{value.split("%")[1]}*'
            if key_type == "parameter":
                query_type = Q("term", params__key=key_name)
                query_val = Q(self.filter_key[comparator][0], params__value=filter_ops[comparator])
            elif key_type == "tag":
                query_type = Q("term", tags__key=key_name)
                query_val = Q(self.filter_key[comparator][0], tags__value=filter_ops[comparator])
            elif key_type == "metric":
                query_type = Q("term", latest_metrics__key=key_name)
                query_val = Q(self.filter_key[comparator][0],
                              latest_metrics__value=filter_ops[comparator])
            if self.filter_key[comparator][1] == "must_not":
                query = query_type & Q('bool', must_not=[query_val])
            else:
                query = query_type & Q('bool', must=[query_val])
            s = s.query('nested', path=type_dict[key_type], query=query)
        return s

    def _get_orderby_clauses(self, order_by_list: List[str], s: Search) -> Search:
        type_dict = {"metric": "latest_metrics", "parameter": "params", "tag": "tags"}
        sort_clauses = []
        if order_by_list:
            for order_by_clause in order_by_list:
                (key_type, key, ascending) = SearchUtils.\
                    parse_order_by_for_search_runs(order_by_clause)
                sort_order = "asc" if ascending else "desc"
                if not SearchUtils.is_attribute(key_type, "="):
                    key_type = type_dict[key_type]
                    sort_clauses.append({f'{key_type}.value':
                                         {'order': sort_order, "nested":
                                          {"path": key_type, "filter":
                                           {"term": {f'{key_type}.key': key}}}}})
                else:
                    sort_clauses.append({key: {'order': sort_order}})
        sort_clauses.append({"start_time": {'order': "desc"}})
        sort_clauses.append({"_id": {'order': "asc"}})
        s = s.sort(*sort_clauses)
        return s

    def _search_runs(self, experiment_ids: List[str], filter_string: str,
                     run_view_type: str, max_results: int = SEARCH_MAX_RESULTS_DEFAULT,
                     order_by: List[str] = None, page_token: str = None,
                     columns_to_whitelist: List[str] = None) -> Tuple[List[Run], str]:

        def compute_next_token(current_size: int) -> str:
            next_token = None
            if max_results == current_size:
                final_offset = offset + max_results
                next_token = SearchUtils.create_page_token(final_offset)
            return next_token
        if max_results > SEARCH_MAX_RESULTS_THRESHOLD:
            raise MlflowException("Invalid value for request parameter max_results. It must be at "
                                  "most {}, but got value {}"
                                  .format(SEARCH_MAX_RESULTS_THRESHOLD, max_results),
                                  INVALID_PARAMETER_VALUE)
        stages = LifecycleStage.view_type_to_stages(run_view_type)
        parsed_filters = SearchUtils.parse_search_filter(filter_string)
        offset = SearchUtils.parse_start_offset_from_page_token(page_token)
        s = Search(index="mlflow-runs").filter("match", experiment_id=experiment_ids[0]) \
            .filter("terms", lifecycle_stage=stages)
        s = self._build_elasticsearch_query(parsed_filters, s)
        s = self._get_orderby_clauses(order_by, s)
        response = s.source(excludes=["metrics.*"])[offset:offset + max_results].execute()
        runs = [self._hit_to_mlflow_run(r) for r in response]
        next_page_token = compute_next_token(len(runs))
        return runs, next_page_token
