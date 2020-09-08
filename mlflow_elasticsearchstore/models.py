import datetime
from elasticsearch.client import IndicesClient
from elasticsearch_dsl import (Document, InnerDoc, Nested, Text,
                               Keyword, Double, Integer, Long, Boolean)

from mlflow.entities import (Experiment, RunTag, Metric, Param,
                             RunData, RunInfo, Run, ExperimentTag)


class ElasticExperimentTag(InnerDoc):
    key = Keyword()
    value = Text()

    def to_mlflow_entity(self) -> ExperimentTag:
        return ExperimentTag(key=self.key,
                             value=self.value)


class ElasticExperiment(Document):
    name = Keyword()
    artifact_location = Text()
    lifecycle_stage = Keyword()
    tags = Nested(ElasticExperimentTag)

    class Index:
        name = 'mlflow-experiments'
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 1
        }

    def to_mlflow_entity(self) -> Experiment:
        return Experiment(
            experiment_id=str(self.meta.id),
            name=self.name,
            artifact_location=self.artifact_location,
            lifecycle_stage=self.lifecycle_stage,
            tags=[t.to_mlflow_entity() for t in self.tags])


class ElasticMetric(InnerDoc):
    key = Keyword()
    value = Double()
    timestamp = Long()
    step = Integer()
    is_nan = Boolean()

    def to_mlflow_entity(self) -> Metric:
        return Metric(
            key=self.key,
            value=self.value if not self.is_nan else float("nan"),
            timestamp=self.timestamp,
            step=self.step)


class ElasticLatestMetric(InnerDoc):
    key = Keyword()
    value = Double()
    timestamp = Long()
    step = Integer()
    is_nan = Boolean()

    def to_mlflow_entity(self) -> Metric:
        return Metric(
            key=self.key,
            value=self.value if not self.is_nan else float("nan"),
            timestamp=self.timestamp,
            step=self.step)


class ElasticParam(InnerDoc):
    key = Keyword()
    value = Keyword()

    def to_mlflow_entity(self) -> Param:
        return Param(
            key=self.key,
            value=self.value)


class ElasticTag(InnerDoc):
    key = Keyword()
    value = Keyword()

    def to_mlflow_entity(self) -> RunTag:
        return RunTag(
            key=self.key,
            value=self.value)


class ElasticRun(Document):
    name = Keyword()
    source_type = Keyword()
    source_name = Keyword()
    experiment_id = Keyword()
    user_id = Keyword()
    status = Keyword()
    start_time = Long()
    end_time = Long()
    source_version = Keyword()
    lifecycle_stage = Keyword()
    artifact_uri = Text()
    metrics = Nested(ElasticMetric)
    latest_metrics = Nested(ElasticLatestMetric)
    params = Nested(ElasticParam)
    tags = Nested(ElasticTag)

    class Index:
        name = 'mlflow-runs'
        settings = {
            "number_of_shards": 2,
            "number_of_replicas": 2
        }

    def to_mlflow_entity(self) -> Run:
        run_info = RunInfo(
            run_uuid=self.meta.id,
            run_id=self.meta.id,
            experiment_id=str(self.experiment_id),
            user_id=self.user_id,
            status=self.status,
            start_time=self.start_time,
            end_time=self.end_time,
            lifecycle_stage=self.lifecycle_stage,
            artifact_uri=self.artifact_uri)

        run_data = RunData(
            metrics=[m.to_mlflow_entity() for m in self.metrics],
            params=[p.to_mlflow_entity() for p in self.params],
            tags=[t.to_mlflow_entity() for t in self.tags])
        return Run(run_info=run_info, run_data=run_data)


class ExperimentIndex():
    name = "mlflow-experiments"
    settings = {
        "number_of_shards": 1,
        "number_of_replicas": 1
    }
    mappings = {
        "properties": {
            "name": {"type": "keyword"},
            "artifact_location": {"type": "text"},
            "lifecycle_stage": {"type": "keyword"},
            "tags": {"type": "flattened"}
        }
    }

    @classmethod
    def init(cls, indices: IndicesClient) -> None:
        if not (indices.exists(index=[cls.name])):
            indices.create(index=cls.name,
                           body={"settings": cls.settings,
                                 "mappings": cls.mappings})


class RunIndex():
    name = "mlflow-runs"
    settings = {
        "number_of_shards": 2,
        "number_of_replicas": 3
    }
    mappings = {
        "properties": {
            "name": {"type": "keyword"},
            "source_type": {"type": "keyword"},
            "source_name": {"type": "keyword"},
            "entry_point_name": {"type": "keyword"},
            "experiment_id": {"type": "keyword"},
            "user_id": {"type": "keyword"},
            "status": {"type": "keyword"},
            "start_time": {"type": "long"},
            "end_time": {"type": "long"},
            "source_version": {"type": "keyword"},
            "artifact_uri": {"type": "text"},
            "lifecycle_stage": {"type": "keyword"},
            "artifact_location": {"type": "text"},
            "latest_metrics": {"type": "flattened"},
            "params": {"type": "flattened"},
            "tags": {"type": "flattened"}
        }
    }

    @classmethod
    def init(cls, indices: IndicesClient) -> None:
        if not (indices.exists(index=[cls.name])):
            indices.create(index=cls.name,
                           body={"settings": cls.settings,
                                 "mappings": cls.mappings})
