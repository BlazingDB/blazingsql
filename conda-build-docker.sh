#!/bin/bash
# usage:   ./conda-build-docker.sh cuda_version python_version token custom_label conda_account
# example: ./conda-build-docker.sh 10.0|10.2 3.6|3.7 123 beta blazingsql-nightly

export WORKSPACE=$PWD

CUDA_VERSION="10.0"
if [ ! -z $1 ]; then
    CUDA_VERSION=$1
fi

PYTHON_VERSION="3.7"
if [ ! -z $2 ]; then
  PYTHON_VERSION=$2
fi

MY_UPLOAD_KEY=""
UPLOAD_BLAZING="0"
if [ ! -z $3 ]; then
  MY_UPLOAD_KEY=$3
  UPLOAD_BLAZING=1
fi

CUSTOM_LABEL=""
if [ ! -z $4 ]; then
  CUSTOM_LABEL=$4
fi

CONDA_USERNAME="blazingsql-nightly"
if [ ! -z $5 ]; then
  CONDA_USERNAME=$5
fi

THE_CMD=./ci/cpu/build.sh
if [ ! -z $6 ]; then
  THE_CMD=$6
fi

docker run --rm -ti \
    --runtime=nvidia \
    -u $(id -u):$(id -g) \
    -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION \
    -e CONDA_USERNAME=$CONDA_USERNAME -e MY_UPLOAD_KEY=$MY_UPLOAD_KEY \
    -e UPLOAD_BLAZING=$UPLOAD_BLAZING -e CUSTOM_LABEL=$CUSTOM_LABEL \
    -e GIT_BRANCH="master" -e SOURCE_BRANCH="master" \
    -e WORKSPACE=$WORKSPACE \
    -v /etc/passwd:/etc/passwd \
    -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
    gpuci/rapidsai-base:cuda${CUDA_VERSION}-ubuntu16.04-gcc5-py${PYTHON_VERSION} \
    $THE_CMD
