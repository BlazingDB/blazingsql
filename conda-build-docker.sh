#!/bin/bash
# usage:   ./conda-build-docker.sh cuda_version python_version conda_upload token
# example: ./conda-build-docker.sh 9.2|10.0|10.1 3.6|3.7 blazingsql-nightly 123

export WORKSPACE=$PWD

CUDA_VERSION="10.0"
if [ ! -z $1 ]; then
    CUDA_VERSION=$1
fi

PYTHON_VERSION="3.7"
if [ ! -z $2 ]; then
  PYTHON_VERSION=$2
fi

docker run --rm -ti \
    --runtime=nvidia \
    -u $(id -u):$(id -g) \
    -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION \
    -e CONDA_UPLOAD=$3 -e MY_UPLOAD_KEY=$4 \
    -e WORKSPACE=$WORKSPACE \
    -v /etc/passwd:/etc/passwd \
    -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
    gpuci/rapidsai-base:cuda${CUDA_VERSION}-centos7-gcc7-py${PYTHON_VERSION} \
    ./ci/gpu/build.sh

