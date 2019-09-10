#!/bin/bash
# usage:   ./conda-upload-docker.sh cuda_version python_version token
# example: ./conda-upload-docker.sh 10.0|9.2 3.6|3.7 123

docker run --rm -v $PWD/:/app/ blazingdb/blazingsql:conda-cuda$1 /app/conda-upload.sh $1 $2 $3
