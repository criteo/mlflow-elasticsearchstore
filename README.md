# mlflow-elasticsearchstore

Mlflow plugin to use ElasticSearch as backend for MLflow tracking service. To use this plugin you need a running instance of Elasticsearch 6.X.

## Development

In a python environment (you can use the one where mlflow is already installed): 

```bash
$ git clone git clone https://github.com/criteo/mlflow-elasticsearchstore.git
$ cd mlflow-elasticsearch
$ pip install .
```

## How To 

mlflow-elasticsearchstore can now be used with the "elasticsearch" scheme, in the same python environment : 

```bash
$ mlflow server --host $MLFLOW_HOST --backend-store-uri elasticsearch://$USER:$PASSWORD@$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT --port $MLFLOW_PORT --default-artifact-root $ARTIFACT_LOCATION
```