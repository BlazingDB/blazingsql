#!/bin/bash
# Copyright (c) 2020, BLAZINGSQL.
###########################################
# BlazingDB CPU conda build script for CI #
###########################################

set -e

export PARALLEL_LEVEL=${PARALLEL_LEVEL:-4}

# Set home to the job's workspace
export PATH=/opt/conda/bin:/usr/local/cuda/bin:$PATH
export HOME=$WORKSPACE

# Switch to project root; also root of repo checkout
cd $WORKSPACE

# Get latest tag and number of commits since tag
export GIT_DESCRIBE_TAG=`git describe --abbrev=0 --tags`
export GIT_DESCRIBE_NUMBER=`git rev-list ${GIT_DESCRIBE_TAG}..HEAD --count`

#export DISTUTILS_DEBUG=1

# If nightly build, append current YYMMDD to version
if [[ "$BUILD_MODE" = "branch" && "$SOURCE_BRANCH" = branch-* ]] ; then
  export VERSION_SUFFIX=`date +%y%m%d`
fi

################################################################################
# SETUP - Check environment
################################################################################

gpuci_logger "Get env"
env
if [ ! -z "$TYPE" ] && [ "$TYPE" == "stable" ]; then
  sed -i "/- rapidsai-nightly/g" /conda/.condarc
else
  echo "  - blazingsql-nightly" >> /conda/.condarc
fi

gpuci_logger "Activate conda env"
. /opt/conda/etc/profile.d/conda.sh
conda activate rapids

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
# BUILD - Conda package builds
################################################################################

gpuci_logger "Build conda pkg for blazingsql"
gpuci_conda_retry build conda/recipes/blazingsql --python=$PYTHON

gpuci_logger "Upload conda pkg for blazingsql"
source ci/cpu/upload.sh

