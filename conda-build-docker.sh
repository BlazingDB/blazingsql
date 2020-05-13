#!/bin/bash
# usage:   ./conda-build-docker.sh cuda_version python_version token conda_upload
# example: ./conda-build-docker.sh 9.2|10.0|10.1 3.6|3.7 123 blazingsql-nightly

export WORKSPACE=$PWD

CUDA_VERSION="10.0"
if [ ! -z $1 ]; then
    CUDA_VERSION=$1
fi

PYTHON_VERSION="3.7"
if [ ! -z $2 ]; then
  PYTHON_VERSION=$2
fi

CONDA_UPLOAD="blazingsql-nightly"
if [ ! -z $4 ]; then
  CONDA_UPLOAD=$4
fi

docker run --rm -ti \
    --runtime=nvidia \
    -u $(id -u):$(id -g) \
    -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION \
    -e CONDA_UPLOAD=$CONDA_UPLOAD -e MY_UPLOAD_KEY=$3 \
    -e UPLOAD_BLAZING=1 -e GIT_BRANCH="master" -e SOURCE_BRANCH="master" \
    -e WORKSPACE=$WORKSPACE \
    -v /etc/passwd:/etc/passwd \
    -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
    gpuci/rapidsai-base:cuda${CUDA_VERSION}-ubuntu16.04-gcc5-py${PYTHON_VERSION} \
    ./ci/cpu/build.sh

