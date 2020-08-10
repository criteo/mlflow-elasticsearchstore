# mlflow-elasticsearchstore

Mlflow plugin to use Elaticsearch as Tracking Store in Mlflow. To use this plugin you need a running instance of Elasticsearch 6.8.

## Installation

In a python environment :

```bash
$ git clone git clone https://github.com/criteo/mlflow-elasticsearchstore.git
$ cd mlflow-elasticsearch
$ pip install .
```

## Launch 

mlflow-elasticsearchstore can now be used with the entrypoint elasticsearch, in the same python environment : 

```bash
$ mlflow server --host $MLFLOW_HOST --backend-store-uri elasticsearch://$USER:$PASSWORD@$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT --port $MLFLOW_PORT --default-artifact-root $ARTIFACT_LOCATION
```