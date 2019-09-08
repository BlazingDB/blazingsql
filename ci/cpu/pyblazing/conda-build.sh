#!/bin/bash

echo "****************************************************"
echo "conda build -c conda-forge -c defaults -c rapidsai -c blazingsql --python=$PYTHON conda/recipes/pyBlazing/"
conda build -c conda-forge -c defaults -c rapidsai -c blazingsql --python=$PYTHON conda/recipes/pyBlazing/

#conda build --label $label -c conda-forge -c $channel -c rapidsai --python=$python --output-folder $CONDA_PREFIX/blazing-build/py${python}_cuda${toolkit} .

