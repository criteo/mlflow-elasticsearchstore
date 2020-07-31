import uuid
from typing import List
from elasticsearch_dsl import Index, Search, Q, connections

from mlflow.store.tracking.abstract_store import AbstractStore
from mlflow.protos.databricks_pb2 import (INVALID_PARAMETER_VALUE, RESOURCE_ALREADY_EXISTS,
                                          INVALID_STATE, RESOURCE_DOES_NOT_EXIST, INTERNAL_ERROR)
from mlflow.entities import (Experiment, RunTag, Metric, Param, RunData, RunInfo,
                             SourceType, RunStatus, Run, ViewType, ExperimentTag,
                             Columns, LifecycleStage)
from mlflow.exceptions import MlflowException
from mlflow.utils.uri import append_to_uri_path

from mlflow_elasticsearchstore.models import (ElasticExperiment, ElasticRun, ElasticMetric,
                                              ElasticParam, ElasticTag, ElasticLatestMetric)


class ElasticsearchStore(AbstractStore):

    ARTIFACTS_FOLDER_NAME = "artifacts"
    DEFAULT_EXPERIMENT_ID = "0"

    def __init__(self, user: str, password: str, host: str, port: str) -> None:

        url = f"{user}:{password}@{host}:{port}"

        connections.create_connection(hosts=[url])

    def create_experiment(self, name: str, artifact_location: str = None) -> str:
        if name is None or name == '':
            raise MlflowException('Invalid experiment name', INVALID_PARAMETER_VALUE)
        experiment = ElasticExperiment(name=name, lifecycle_stage=LifecycleStage.ACTIVE,
                                       artifact_location=artifact_location)
        experiment.save()
        return str(experiment.meta.id)

    def get_experiment(self, experiment_id: str) -> Experiment:
        experiment = ElasticExperiment.get(id=experiment_id)
        return experiment.to_mlflow_entity()

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

    def get_run(self, run_id: str) -> Run:
        run = self._get_run(run_id=run_id)
        return run.to_mlflow_entity()

    def _get_run(self, run_id: str) -> ElasticRun:
        run = ElasticRun.get(id=run_id)
        return run

    def log_metric(self, run_id: str, metric: Metric) -> None:
        run = self._get_run(run_id=run_id)
        new_metric = ElasticMetric(key=metric.key,
                                   value=metric.value,
                                   timestamp=metric.timestamp,
                                   step=metric.step)
        run.metrics.append(new_metric)
        run.save()

    def log_param(self, run_id: str, param: Param) -> None:
        run = self._get_run(run_id=run_id)
        new_param = ElasticParam(key=param.key,
                                 value=param.value)
        run.params.append(new_param)
        run.save()

    def set_tag(self, run_id: str, tag: RunTag) -> None:
        run = self._get_run(run_id=run_id)
        new_tag = ElasticTag(key=tag.key,
                             value=tag.value)
        run.tags.append(new_tag)
        run.save()

    def search_runs(self, experiment_ids: List[str]) -> List[Run]:
        response = Search(index="mlflow-runs").filter("match",
                                                      experiment_id=experiment_ids[0]).execute()
        runs = []
        for r in response:
            metrics = []
            params = []
            tags = []
            for m in r.metrics:
                metrics.append(ElasticMetric(key=m.key,
                                             value=m.value,
                                             timestamp=m.timestamp,
                                             step=m.step))
            for p in r.params:
                params.append(ElasticParam(key=p.key, value=p.value))
            for t in r.tags:
                tags.append(ElasticTag(key=t.key, value=t.value))
            run = ElasticRun(meta={'id': r.meta.id},
                             experiment_id=r.experiment_id, user_id=r.user_id,
                             status=r.status,
                             start_time=r.start_time,
                             lifecycle_stage=r.lifecycle_stage, artifact_uri=r.artifact_uri,
                             metrics=metrics, params=params, tags=tags
                             )
            runs.append(run.to_mlflow_entity())
        return runs
