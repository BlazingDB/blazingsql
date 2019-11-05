#!/bin/bash
# usage:   ./conda-build-docker.sh cuda_version python_version conda_build conda_upload token
# example: ./conda-build-docker.sh 9.2|10.0 3.6|3.7 blazingsql-nightly blazingsql-nightly 123
export WORKSPACE=$PWD

CUDA_VERSION="9.2"
if [ ! -z $1 ]; then
    CUDA_VERSION=$1
fi

PYTHON_VERSION="3.7"
if [ ! -z $2 ]; then
    PYTHON_VERSION=$2
fi

docker run --rm \
    -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION \
    -e CONDA_BUILD=$3 -e CONDA_UPLOAD=$4 -e MY_UPLOAD_KEY=$5 \
    -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
    gpuci/miniconda-cuda:${CUDA_VERSION}-devel-ubuntu16.04 \
    ./ci/cpu/build.sh
