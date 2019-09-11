#!/bin/bash
# usage:   ./conda-build-docker.sh cuda_version python_version
# example: ./conda-build-docker.sh 9.2|10.0 3.6|3.7
export WORKSPACE=$PWD

CUDA_VERSION="9.2"
if [ ! -z $1 ]; then
    CUDA_VERSION=$1
fi

 PYTHON_VERSION="3.7"
if [ ! -z $2 ]; then
    PYTHON_VERSION=$2
fi

docker run -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION -e MY_UPLOAD_KEY=$3 --rm -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} gpuci/miniconda-cuda:${CUDA_VERSION}-devel-ubuntu16.04 ./ci/cpu/build.sh
