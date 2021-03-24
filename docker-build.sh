#!/bin/bash
# Usage:   cuda_version   python_version  rapids_version  nightly
# Example: 10.1|10.2|11.0 3.7|3.8         0.15|0.16       nightly|true

CUDA_VERSION="10.1"
if [ ! -z $1 ]; then
  CUDA_VERSION=$1
fi

PYTHON_VERSION="3.7"
if [ ! -z $2 ]; then
  PYTHON_VERSION=$2
fi

RAPIDS_VERSION="0.18"
if [ ! -z $3 ]; then
  RAPIDS_VERSION=$3
fi

NIGHTLY=""
if [ ! -z $4 ]; then
  NIGHTLY="-nightly"
fi

DOCKER_IMAGE="blazingdb/blazingsql$NIGHTLY:cuda$CUDA_VERSION-py$PYTHON_VERSION"
CONDA_CH="-c blazingsql$NIGHTLY -c rapidsai$NIGHTLY -c nvidia"

CMD="docker build -t ${DOCKER_IMAGE} --build-arg CUDA_VER=${CUDA_VERSION} --build-arg PYTHON_VERSION=${PYTHON_VERSION} --build-arg CONDA_CH=\"${CONDA_CH}\" --build-arg RAPIDS_VERSION=\"${RAPIDS_VERSION}\" ./"

echo "CMD: $CMD"
eval $CMD

