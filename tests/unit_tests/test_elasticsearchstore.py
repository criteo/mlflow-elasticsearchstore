import pytest
import mock

import mlflow_elasticsearchstore
from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import ElasticExperiment, ElasticRun, \
    ElasticMetric, ElasticParam, ElasticTag


@mock.patch('mlflow_elasticsearchstore.elasticsearch_store.ElasticsearchStore')
@mock.patch('mlflow_elasticsearchstore.models.ElasticExperiment')
def test_create_experiment(elastic_experiment_mock, elasticsearch_store_mock):
    elasticsearch_store_mock.create_experiment("name", "artifact_location")
    elastic_experiment_mock.assert_called_once_with()


@mock.patch('mlflow_elasticsearchstore.elasticsearch_store.ElasticsearchStore')
@mock.patch('mlflow_elasticsearchstore.models.ElasticRun')
def test__get_run(elastic_run_mock, elasticsearch_store_mock):
    elasticsearch_store_mock.test__get_run(1)
    elastic_run_mock.assert_called_once_with(id=1)
    elastic_run_mock.return_value.get
