#!/bin/bash
# usage:   ./conda-build-docker.sh conda_build conda_upload python_version token
# example: ./conda-build-docker.sh blazingsql-nightly blazingsql-nightly 9.2|10.0 3.6|3.7 123
export WORKSPACE=$PWD

PYTHON_VERSION="3.7"
if [ ! -z $1 ]; then
    PYTHON_VERSION=$3
fi

docker run --rm -e CONDA_BUILD=$1 -e CONDA_UPLOAD=$2 -e PYTHON=$PYTHON_VERSION -e MY_UPLOAD_KEY=$4 -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} gpuci/miniconda-cuda:10.0-devel-ubuntu16.04 ./ci/cpu/build.sh
