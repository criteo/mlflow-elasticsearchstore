#!/bin/bash

docker ps -a
docker rm -f $(docker ps -a --format '{{.Names}}' | grep elasticsearch)
docker-compose up