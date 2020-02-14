#!/bin/bash

set -e

SOURCE_BRANCH="master"

export BLAZINGSQL_FILE=`conda build conda/recipes/blazingsql/ --python=$PYTHON --output`

# Restrict uploads to master branch
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
    echo "LABEL_OPTION=${LABEL_OPTION}"

    test -e ${BLAZINGSQL_FILE}
    echo "Upload blazingsql: "${BLAZINGSQL_FILE}
    anaconda -t ${MY_UPLOAD_KEY} upload -u ${CONDA_USERNAME} ${LABEL_OPTION} --force ${BLAZINGSQL_FILE}
fi
