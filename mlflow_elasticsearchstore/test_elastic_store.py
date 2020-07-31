import time
import datetime
import fire

from mlflow.entities import Metric, Param, RunTag

from mlflow_elasticsearchstore.elasticsearch_store import ElasticsearchStore
from mlflow_elasticsearchstore.models import ElasticExperiment, ElasticRun, ElasticMetric, \
    ElasticParam, ElasticTag, ElasticLatestMetric

metric = Metric("metric2", 9, int(datetime.datetime.now().timestamp() * 1000), 0)
param = Param("param2", "val1")
tag = RunTag("tag2", "val1")


def main(user: str, password: str, host: str, port: str) -> None:
    store = ElasticsearchStore(user, password, host, port)
    exp_id = store.create_experiment("a.vivien-exp2", "artifact_path")
    time.sleep(1)
    run = store.create_run(exp_id, "1", int(datetime.datetime.now().timestamp() * 1000), [])
    time.sleep(1)
    store.log_metric(run._info._run_id, metric)
    store.log_param(run._info._run_id, param)
    store.set_tag(run._info._run_id, tag)
    time.sleep(1)
    ml_run = store.get_run(run._info._run_id)
    print(ml_run)
    runs = store.search_runs([exp_id])
    print(runs)


if __name__ == "__main__":
    fire.Fire(main)
