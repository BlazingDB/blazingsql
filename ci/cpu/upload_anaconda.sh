#!/bin/bash

set -e

export PYBLAZING_FILE=`conda build conda/recipes/blazingsql/ --python=$PYTHON --output`

LABEL_OPTION="--label main --label cuda"$CUDA_VER
echo "LABEL_OPTION=${LABEL_OPTION}"

if [ -z "$MY_UPLOAD_KEY" ]; then
    echo "No upload key"
    return 0
fi

test -e ${PYBLAZING_FILE}
echo "Upload pyblazing"
echo ${PYBLAZING_FILE}

anaconda -t ${MY_UPLOAD_KEY} upload -u ${CONDA_UPLOAD} ${LABEL_OPTION} --force ${PYBLAZING_FILE}

