#!/bin/bash

set -e

export LIB_FILE=`conda build conda/recipes/libbsql-engine/ --output`
export ENGINE_FILE=`conda build conda/recipes/bsql-engine/ --output`

LABEL_OPTION="--label main --label cuda"$CUDA_VER
echo "LABEL_OPTION=${LABEL_OPTION}"

if [ -z "$MY_UPLOAD_KEY" ]; then
    echo "No upload key"
    return 0
fi

test -e ${RAL_FILE}
echo "Upload ral"
echo ${RAL_FILE}

anaconda -t ${MY_UPLOAD_KEY} upload -u ${CONDA_UPLOAD} ${LABEL_OPTION} --force ${LIB_FILE}
anaconda -t ${MY_UPLOAD_KEY} upload -u ${CONDA_UPLOAD} ${LABEL_OPTION} --force ${ENGINE_FILE}
