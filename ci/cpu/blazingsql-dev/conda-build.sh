#!/bin/bash

if [ -z "$CONDA_BUILD" ]; then
    CONDA_BUILD="blazingsql"
fi

conda build -c ${CONDA_BUILD} -c rapidsai -c conda-forge -c defaults --python=$PYTHON conda/recipes/blazingsql-dev/
