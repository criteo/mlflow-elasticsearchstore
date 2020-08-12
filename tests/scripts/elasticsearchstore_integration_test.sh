#!/bin/bash

#get directory of current file
DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi

echo "Beginning Elasticsearchstore integration tests"

echo "Creating venv"
rm -rf elasticsearchstore_test_env
python3.6 -m venv elasticsearchstore_test_env
. elasticsearchstore_test_env/bin/activate
pip install -U pip setuptools
pip install -e .
pip install -r tests-requirements.txt

echo "Launching MLflow server"
mlflow server --host 0.0.0.0 --port 5005 --backend-store-uri elasticsearch://127.0.0.1:9200--default-artifact-root $_MLFLOW_SERVER_ARTIFACT_ROOT &
