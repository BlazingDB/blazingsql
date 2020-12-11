#!/bin/bash
#
# Adopted from https://github.com/tmcdonell/travis-scripts/blob/dfaac280ac2082cd6bcaba3217428347899f2975/update-accelerate-buildbot.sh

set -e

# Setup 'gpuci_retry' for upload retries (results in 4 total attempts)
export GPUCI_RETRY_MAX=3
export GPUCI_RETRY_SLEEP=30

# Set default label options if they are not defined elsewhere
export LABEL_OPTION=${LABEL_OPTION:-"--label main"}

# Skip uploads unless BUILD_MODE == "branch"
if [ ${BUILD_MODE} != "branch" ]; then
  echo "Skipping upload"
  return 0
fi

# Skip uploads if there is no upload key
if [ -z "$MY_UPLOAD_KEY" ]; then
  echo "No upload key"
  return 0
fi

################################################################################
# SETUP - Get conda file output locations
################################################################################

export BLAZINGSQL_FILE=`conda build conda/recipes/blazingsql/ --python=$PYTHON --output`

################################################################################
# UPLOAD - Conda packages
################################################################################
echo "### UPLOAD_BLAZING: $UPLOAD_BLAZING"
if [ "$UPLOAD_BLAZING" == "1" ]; then
    LABEL_OPTION="--label main"
    if [ ! -z "$CUSTOM_LABEL" ]; then
        LABEL_OPTION="--label "$CUSTOM_LABEL
    fi
    echo "LABEL_OPTION=${LABEL_OPTION}"

    if [ ${CONDA_USERNAME} == "blazingsql" ]; then
        RAPIDS_CONDA_USERNAME="rapidsai"
        RAPIDS_CONDA_KEY=${RAPIDS_UPLOAD_KEY}
    elif [ ${CONDA_USERNAME} == "blazingsql-nightly" ]; then
        RAPIDS_CONDA_USERNAME="rapidsai-nightly"
        RAPIDS_CONDA_KEY=${RAPIDS_NIGHTLY_UPLOAD_KEY}
    fi

    test -e ${BLAZINGSQL_FILE}

    echo "Upload BlazingSQL to ${CONDA_USERNAME} channel: ${BLAZINGSQL_FILE}"
    anaconda -t ${MY_UPLOAD_KEY} upload -u ${CONDA_USERNAME} ${LABEL_OPTION} --force ${BLAZINGSQL_FILE}

    #if [ ! -z "$RAPIDS_CONDA_USERNAME" ]; then
    #    echo "Upload BlazingSQL to ${RAPIDS_CONDA_USERNAME} channel: ${BLAZINGSQL_FILE}"
    #    anaconda -t ${RAPIDS_CONDA_KEY} upload -u ${RAPIDS_CONDA_USERNAME} ${LABEL_OPTION} --force ${BLAZINGSQL_FILE}
    #fi
fi
