#!/bin/bash

docker ps -a
docker images

docker build  --tag elastic_store .
docker run \
    -p 9200:9200 \
<<<<<<< HEAD
    -p 9300:9300 \
    -e "discovery.type=single-node" \
=======
>>>>>>> 9d0315a... col_to_whitelist
    -e ELASTIC_PASSWORD=password \
    -e xpack.security.enabled=true \
    -d \
    elastic_store