#!/bin/bash

set -e

export CALCITE_FILE=`conda build conda/recipes/bsql-algebra/ --output`

LABEL_OPTION="--label main"
echo "LABEL_OPTION=${LABEL_OPTION}"

if [ -z "$MY_UPLOAD_KEY" ]; then
    echo "No upload key"
    return 0
fi

test -e ${CALCITE_FILE}
echo "Upload calcite"
echo ${CALCITE_FILE}

anaconda -t ${MY_UPLOAD_KEY} upload -u ${CONDA_UPLOAD} ${LABEL_OPTION} --force ${CALCITE_FILE}
