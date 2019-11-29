#!/bin/bash

set -e

export COMMUNICATION_FILE=`conda build conda/recipes/bsql-comms/ --output`

LABEL_OPTION="--label main --label cuda"$CUDA_VER
echo "LABEL_OPTION=${LABEL_OPTION}"

if [ -z "$MY_UPLOAD_KEY" ]; then
    echo "No upload key"
    return 0
fi

if [ -z "$CONDA_UPLOAD" ]; then
    CONDA_UPLOAD="blazingsql"
fi

test -e ${COMMUNICATION_FILE}
echo "Upload communication"
echo ${COMMUNICATION_FILE}

anaconda -t ${MY_UPLOAD_KEY} upload -u ${CONDA_UPLOAD} ${LABEL_OPTION} --force ${COMMUNICATION_FILE}
