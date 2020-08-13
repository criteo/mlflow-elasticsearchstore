import pytest

from mlflow.entities import Experiment

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore

actual_experiment = Experiment(experiment_id="WrAh43MB26mjXDfbtLks", name="a.vivien-exp",
                               lifecycle_stage="active", artifact_location="artifact_path", tags=[])


@pytest.mark.integration
@pytest.mark.usefixtures('init_store')
def test_get_experiment(init_store):
    experiment = init_store.get_experiment(experiment_id=actual_experiment.experiment_id)
    assert experiment.__dict__ == actual_experiment.__dict__
