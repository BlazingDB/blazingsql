#!/bin/bash
# usage:   ./conda-build-docker.sh cuda_version python_version
# example: ./conda-build-docker.sh 9.2|10.0 3.6|3.7

docker run --rm -u $(id -u):$(id -g) -v $PWD/:/app/ blazingdb/blazingsql:conda-cuda$1 /app/conda-build.sh $1 $2
