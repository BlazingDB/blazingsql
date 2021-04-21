#!/bin/bash
# Copyright (c) 2018, NVIDIA CORPORATION.
##############################################
# BlazingDB GPU build and test script for CI #
##############################################
set -e
NUMARGS=$#
ARGS=$*

# Arg parsing function
function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

# Set path and build parallel level
export PATH=/opt/conda/bin:/usr/local/cuda/bin:$PATH
export PARALLEL_LEVEL=${PARALLEL_LEVEL:-4}
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

gpuci_logger "Check environment"
env
echo "  - blazingsql-nightly" >> /conda/.condarc

gpuci_logger "Check GPU usage"
nvidia-smi

gpuci_logger "Activate conda env"
conda create python=$PYTHON_VER -y -n bsql
source activate bsql
conda config --set ssl_verify False

gpuci_logger "Installing BlazingSQL dev environment"

# NOTE: needing to manually install spdlog here because v1.8 is causing issues https://github.com/gabime/spdlog/issues/1662

gpuci_logger "Install Dependencies"
${WORKSPACE}/dependencies.sh ${MINOR_VERSION} ${CUDA_REL} nightly

gpuci_logger "Check versions"
python --version
$CC --version
$CXX --version

gpuci_logger "Conda Information"
conda info
conda config --show-sources
conda list --show-channel-urls
conda config --set ssl_verify False
################################################################################
# BUILD - Build from Source
################################################################################

gpuci_logger "Build BlazingSQL"
#export DISTUTILS_DEBUG=1
${WORKSPACE}/build.sh

################################################################################
# TEST - Run Tests
################################################################################

if hasArg --skip-tests; then
    gpuci_logger "Skipping Tests"
else
    INSTALL_PREFIX=${INSTALL_PREFIX:=${PREFIX:=${CONDA_PREFIX}}}
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$INSTALL_PREFIX/lib:$INSTALL_PREFIX/lib64

    gpuci_logger "Check GPU usage"
    nvidia-smi

    export BLAZINGSQL_E2E_IN_GPUCI_ENV="true"
    export BLAZINGSQL_E2E_SAVE_LOG=true
    ${WORKSPACE}/ci/gpu/test.sh
fi
