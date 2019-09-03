#!/bin/bash
# usage:   ./conda-build-docker.sh cuda_version python_version build_number
# example: ./conda-build-docker.sh 10.0|9.2     3.7|3.6        123

docker run --rm -u $(id -u):$(id -g) -v $PWD/:/app/ blazingdb/blazingsql:conda-cuda$1 /app/conda-build.sh $1 $2 $3
