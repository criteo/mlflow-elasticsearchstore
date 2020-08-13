import pytest
import mock
from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore


@pytest.mark.integration_test
def test_get_experiment():
    store = ElasticsearchStore(store_uri="elasticsearch://elastic:password@localhost:9200",
                               artifact_uri="viewfs://preprod-pa4/user/mlflow/mlflow_artifacts")
    experiment_id = "WrAh43MB26mjXDfbtLks"
    experiment = store.get_experiment(experiment_id=experiment_id)
    assert experiment.experiment_id == experiment_id
