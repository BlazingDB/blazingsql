#!/bin/bash
# usage:   ./conda-build-docker.sh conda_build conda_upload token
# example: ./conda-build-docker.sh blazingsql-nightly blazingsql-nightly 123

export WORKSPACE=$PWD

CONDA_RC=$PWD/.condarc
CONDA_PKGS=$PWD/conda_pkgs/
CONDA_CACHE=$PWD/conda_cache/

if [ ! -f "$CONDA_RC" ]; then
    touch $CONDA_RC
fi
mkdir -p $CONDA_PKGS $CONDA_CACHE

docker run --rm \
    -u $(id -u):$(id -g) \
    -e CONDA_BUILD=$1 -e CONDA_UPLOAD=$2 -e MY_UPLOAD_KEY=$3 \
    -v $CONDA_RC:/.condarc \
    -v $CONDA_PKGS:/opt/conda/pkgs/ \
    -v $CONDA_CACHE:/.cache/ \
    -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
    gpuci/miniconda-cuda:10.0-devel-ubuntu16.04 \
    ./ci/cpu/build.sh
