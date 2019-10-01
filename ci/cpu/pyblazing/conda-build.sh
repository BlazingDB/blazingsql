#!/bin/bash

echo "****************************************************"
echo "conda build -c blazingsql/label/main -c rapidsai -c conda-forge -c defaults --python=$PYTHON conda/recipes/pyBlazing/"
conda build -c blazingsql-nightly -c rapidsai-nightly -c conda-forge -c defaults --python=$PYTHON conda/recipes/pyBlazing/

