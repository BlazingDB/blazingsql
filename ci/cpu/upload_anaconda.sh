#!/bin/bash

set -e

SOURCE_BRANCH="main"

export BLAZINGSQL_FILE=`conda build conda/recipes/blazingsql/ --python=$PYTHON --output`

# Restrict uploads to main branch
if [ ${GIT_BRANCH} != ${SOURCE_BRANCH} ]; then
    echo "Skipping upload"
    return 0
fi

if [ -z "$MY_UPLOAD_KEY" ]; then
    echo "No upload key"
    return 0
fi

if [ "$UPLOAD_BLAZING" == "1" ]; then
    LABEL_OPTION="--label main --label cuda"$CUDA
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

    echo "Upload BlazingSQL to ${RAPIDS_CONDA_USERNAME} channel: ${BLAZINGSQL_FILE}"
    anaconda -t ${RAPIDS_CONDA_KEY} upload -u ${RAPIDS_CONDA_USERNAME} ${LABEL_OPTION} --force ${BLAZINGSQL_FILE}
fi
