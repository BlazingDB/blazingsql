#!/bin/bash
# Copyright (c) 2019, BLAZINGSQL.
######################################
# cuDF CPU conda build script for CI #
######################################
set -e

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

# Set home to the job's workspace
export HOME=$WORKSPACE

# Switch to project root; also root of repo checkout
cd $WORKSPACE

# Get latest tag and number of commits since tag
export GIT_DESCRIBE_TAG=`git describe --abbrev=0 --tags`
export GIT_DESCRIBE_NUMBER=`git rev-list ${GIT_DESCRIBE_TAG}..HEAD --count`

# Nightly seccion
echo "IS_NIGHTLY" $IS_NIGHTLY
if [ $IS_NIGHTLY == "true" ]; then
      NIGHTLY="-nightly"
     # CUDF="cudf=0.10"

      libcudf="libcudf=0.10.0a191009"
      nvstrings="nvstrings=0.10.0a191009"
      rmm="rmm=0.10.0a191009"
      daskcudf="dask-cudf=0.10.0a191009"
      #Replazing cudf version

      echo "Replacing cudf version into meta.yaml"
      sed -ie "s/libcudf/$libcudf/g" conda/recipes/pyBlazing/meta.yaml
      sed -ie "s/nvstrings/$nvstrings/g" conda/recipes/pyBlazing/meta.yaml
      sed -ie "s/rmm/$rmm/g" conda/recipes/pyBlazing/meta.yaml
      sed -ie "s/dask-cudf/$daskcudf/g" conda/recipes/pyBlazing/meta.yaml
fi

################################################################################
# SETUP - Check environment
################################################################################

logger "Get env..."
env

logger "Check versions..."
python --version
gcc --version
g++ --version
conda list

# FIX Added to deal with Anancoda SSL verification issues during conda builds
conda config --set ssl_verify False

logger "Install dependencies..."
conda install -y conda-build anaconda-client

################################################################################
# BUILD - Conda package builds
################################################################################

logger "Build conda pkg for pyblazing..."
source ci/cpu/pyblazing/conda-build.sh

logger "Upload conda pkg for pyblazing..."
source ci/cpu/upload_anaconda.sh
