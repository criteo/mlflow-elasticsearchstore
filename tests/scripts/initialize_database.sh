#!/bin/bash

docker ps -a
docker images

docker build --tag elastic_store .
docker run --network=host elastic_store