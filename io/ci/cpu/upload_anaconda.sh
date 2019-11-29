#!/bin/bash

set -e

export IO_FILE=`conda build conda/recipes/bsql-io/ --output`

LABEL_OPTION="--label main"
echo "LABEL_OPTION=${LABEL_OPTION}"

if [ -z "$MY_UPLOAD_KEY" ]; then
    echo "No upload key"
    return 0
fi

test -e ${IO_FILE}
echo "Upload io"
echo ${IO_FILE}

anaconda -t ${MY_UPLOAD_KEY} upload -u ${CONDA_UPLOAD} ${LABEL_OPTION} --force ${IO_FILE}
