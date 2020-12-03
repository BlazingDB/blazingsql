#!/bin/bash
# Copyright (c) 2019, BLAZINGSQL.
###########################################
# BlazingDB CPU conda build script for CI #
###########################################
set -e

export PARALLEL_LEVEL=4

# Set home to the job's workspace
export PATH=/usr/local/cuda/bin:$PATH:/conda/bin
export HOME=$WORKSPACE

# Switch to project root; also root of repo checkout
cd $WORKSPACE

# Get latest tag and number of commits since tag
export GIT_DESCRIBE_TAG=`git describe --abbrev=0 --tags`
export GIT_DESCRIBE_NUMBER=`git rev-list ${GIT_DESCRIBE_TAG}..HEAD --count`

#export DISTUTILS_DEBUG=1

################################################################################
# SETUP - Check environment
################################################################################

logger "Get env..."
env
echo "  - blazingsql-nightly" >> /conda/.condarc

logger "Activate conda env..."
source activate gdf

logger "Check versions..."
python --version
gcc --version
g++ --version
conda list

# FIX Added to deal with Anancoda SSL verification issues during conda builds
conda config --set ssl_verify False

################################################################################
# BUILD - Conda package builds
################################################################################

logger "Build conda pkg for blazingsql..."
source ci/cpu/blazingsql/build_blazingsql.sh

logger "Upload conda pkg for blazingsql..."
source ci/cpu/upload_anaconda.sh
