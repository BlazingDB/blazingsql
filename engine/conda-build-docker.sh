#!/bin/bash
# usage:   ./conda-build-docker.sh cuda_version python_version rapids-version conda_build conda_upload token
# example: ./conda-build-docker.sh 9.2|10.0 3.6|3.7 0.10|0.11 blazingsql-nightly,rapidsai-nightly blazingsql-nightly 123

export WORKSPACE=$PWD

CONDA_RC=$PWD/.condarc
CONDA_PKGS=$PWD/conda_pkgs/
CONDA_CACHE=$PWD/conda_cache/

if [ ! -f "$CONDA_RC" ]; then
    touch $CONDA_RC
fi
mkdir -p $CONDA_PKGS $CONDA_CACHE

CUDA_VERSION="9.2"
if [ ! -z $1 ]; then
    CUDA_VERSION=$1
fi

PYTHON_VERSION="3.7"
if [ ! -z $2 ]; then
  PYTHON_VERSION=$2
fi

docker run --rm \
    --runtime=nvidia \
    -u $(id -u):$(id -g) \
    -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION -e RAPIDS_BUILD_VERSION=$3 \
    -e CONDA_BUILD=$4 -e CONDA_UPLOAD=$5 -e MY_UPLOAD_KEY=$6 \
    -v $CONDA_RC:/.condarc \
    -v $CONDA_PKGS:/opt/conda/pkgs/ \
    -v $CONDA_CACHE:/.cache/ \
    -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
    gpuci/miniconda-cuda:${CUDA_VERSION}-devel-ubuntu16.04 \
    ./ci/cpu/build.sh

