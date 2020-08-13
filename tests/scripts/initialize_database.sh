#!/bin/bash

docker ps -a
docker images

docker build --tag elastic_store .
docker run \
    --network=bridge \
    -p 9200:9200 \
    -e ELASTIC_PASSWORD=password \
    -e xpack.security.enabled=true \
    -d \
    elastic_store