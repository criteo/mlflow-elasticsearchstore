#!/bin/bash

docker ps -a
docker images

docker build --tag elastic_store .
docker run \
    --network=host \
    -e ELASTIC_PASSWORD=password \
    -e xpack.security.enabled=true \
    -d \
    elastic_store
