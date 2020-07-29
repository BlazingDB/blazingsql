#!/bin/bash
# Copyright (c) 2018, NVIDIA CORPORATION.
##############################################
# BlazingDB GPU build and test script for CI #
##############################################
set -e
NUMARGS=$#
ARGS=$*

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

# Arg parsing function
function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

# Set path and build parallel level
export PATH=/usr/local/cuda/bin:$PATH:/conda/bin
export PARALLEL_LEVEL=4
export CUDA_REL=${CUDA_VERSION%.*}

# Set home to the job's workspace
export HOME=$WORKSPACE

# Parse git describe
cd $WORKSPACE
export GIT_DESCRIBE_TAG=`git describe --tags`
export MINOR_VERSION=`echo $GIT_DESCRIBE_TAG | grep -o -E '([0-9]+\.[0-9]+)'`

# Set `LIBCUDF_KERNEL_CACHE_PATH` environment variable to $HOME/.jitify-cache because
# it's local to the container's virtual file system, and not shared with other CI jobs
# like `/tmp` is.
export LIBCUDF_KERNEL_CACHE_PATH="$HOME/.jitify-cache"

################################################################################
# SETUP - Check environment
################################################################################

logger "Check environment..."
env
echo "  - blazingsql-nightly" >> /conda/.condarc

logger "Check GPU usage..."
nvidia-smi

logger "Activate conda env..."
conda create python=$PYTHON_VER -y -n bsql
source activate bsql

echo "Installing BlazingSQL dev environment"

# install deps
echo "conda install --yes -c conda-forge google-cloud-cpp ninja"
conda install --yes -c conda-forge google-cloud-cpp ninja
echo "BlazingSQL dev basic deps installed"

# NOTE cython must be the same of cudf (for 0.11 and 0.12 cython is >=0.29,<0.30)
echo "conda install --yes openjdk=8.0 maven cmake gtest gmock rapidjson cppzmq cython=0.29 jpype1 netifaces pyhive"
conda install --yes openjdk=8.0 maven cmake gtest gmock rapidjson cppzmq cython=0.29 jpype1 netifaces pyhive
echo "BlazingSQL deps installed"

# install cudf
echo "conda install --yes dask-cuda=${MINOR_VERSION} dask-cudf=${MINOR_VERSION} cudf=${MINOR_VERSION} python=$PYTHON_VER cudatoolkit=$CUDA_REL"
conda install --yes dask-cuda=${MINOR_VERSION} dask-cudf=${MINOR_VERSION} ucx-py=${MINOR_VERSION} cudf=${MINOR_VERSION} python=$PYTHON_VER cudatoolkit=$CUDA_REL
echo "cudf and other rapids dependencies installed"

# install end to end tests dependencies
echo "conda install --yes openjdk=8.0 maven pyspark=3.0.0 pytest"
conda install --yes openjdk=8.0 maven pyspark=3.0.0 pytest

echo "pip install pydrill openpyxl pymysql gitpython pynvml gspread oauth2client"
pip install pydrill openpyxl pymysql gitpython pynvml gspread oauth2client
echo "BlazingSQL end to end tests dependencies installed"

logger "Check versions..."
python --version
$CC --version
$CXX --version
conda list

################################################################################
# BUILD - Build from Source
################################################################################

logger "Build BlazingSQL"
#export DISTUTILS_DEBUG=1
${WORKSPACE}/build.sh

################################################################################
# TEST - Run Tests
################################################################################

if hasArg --skip-tests; then
    logger "Skipping Tests..."
else
    INSTALL_PREFIX=${INSTALL_PREFIX:=${PREFIX:=${CONDA_PREFIX}}}
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$INSTALL_PREFIX/lib

    logger "Check GPU usage..."
    nvidia-smi

    export BLAZINGSQL_E2E_IN_GPUCI_ENV="true"
    export BLAZINGSQL_E2E_SAVE_LOG=true
    ${WORKSPACE}/ci/gpu/test.sh
fi
