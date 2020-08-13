#!/bin/bash

docker ps -a
docker rm -f $(docker ps -a --format '{{.Names}}' | grep elastic_store)
docker build --tag elastic_store .
docker-compose up -d