#!/bin/bash

docker ps -a
docker images

docker build  --tag elastic_store .
docker run \
    --network=host \
    -e "discovery.type=single-node" \
    -e ELASTIC_PASSWORD=password \
    -e xpack.security.enabled=true \
    -d \
    elastic_store