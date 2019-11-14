#!/bin/bash

echo "CMD: conda build ${CONDA_CH} -c blazingsql/label/main/ -c rapidsai -c conda-forge -c defaults --python=$PYTHON conda/recipes/blazingsql/"
conda build ${CONDA_CH} -c blazingsql/label/main/ -c rapidsai -c conda-forge -c defaults --python=$PYTHON conda/recipes/blazingsql/

