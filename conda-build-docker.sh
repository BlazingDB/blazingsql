#!/bin/bash
# usage:   ./conda-build-docker.sh python_version conda_build conda_upload token
# example: ./conda-build-docker.sh 36|3.7 blazingsql-nightly blazingsql-nightly 123
export WORKSPACE=$PWD

PYTHON_VERSION="3.7"
if [ ! -z $1 ]; then
    PYTHON_VERSION=$1
fi

docker run --rm -e PYTHON=$PYTHON_VERSION -e CONDA_BUILD=$2 -e CONDA_UPLOAD=$3 -e MY_UPLOAD_KEY=$4 -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} gpuci/miniconda-cuda:10.0-devel-ubuntu16.04 ./ci/cpu/build.sh
