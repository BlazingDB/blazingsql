#!/bin/bash
# usage:   ./conda-build-docker.sh conda_build conda_upload cuda_version python_version token
# example: ./conda-build-docker.sh blazingsql-nightly blazingsql-nightly 9.2|10.0 3.6|3.7 123

export WORKSPACE=$PWD

CUDA_VERSION="9.2"
if [ ! -z $3 ]; then
    CUDA_VERSION=$3
fi

PYTHON_VERSION="3.7"
if [ ! -z $4 ]; then
    PYTHON_VERSION=$4
fi

docker run -e CONDA_BUILD=$1 -e CONDA_UPLOAD=$2 -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION -e MY_UPLOAD_KEY=$5 --rm -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} gpuci/miniconda-cuda:${CUDA_VERSION}-devel-ubuntu16.04 ./ci/cpu/build-dev.sh
