#!/bin/bash

docker ps -a
docker images

docker build --tag elastic_store .
docker-compose up -d