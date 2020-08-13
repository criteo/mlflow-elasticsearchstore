import pytest
import mock
from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore


@pytest.mark.integtest
def test_get_experiment():
    store = ElasticsearchStore(store_uri="elasticsearch://elastic:password@localhost:9200",
                               artifact_uri="viewfs://preprod-pa4/user/mlflow/mlflow_artifacts")
    experiment_id = "FkpsfafgTAez5z_KUQyPvw"
    experiment = store.get_experiment(experiment_id=experiment_id)
    assert experiment.meta.id == experiment_id
