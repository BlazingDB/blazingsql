#!/bin/bash
# usage:   ./conda-build-docker.sh python_version token nightly
# example: ./conda-build-docker.sh 9.2|10.0 3.6|3.7 123 true
export WORKSPACE=$PWD

 PYTHON_VERSION="3.7"
if [ ! -z $1 ]; then
    PYTHON_VERSION=$1
fi

docker run -e PYTHON=$PYTHON_VERSION -e MY_UPLOAD_KEY=$2 -e IS_NIGHTLY=$3 --rm -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} gpuci/miniconda-cuda:10.0-devel-ubuntu16.04 ./ci/cpu/build.sh
