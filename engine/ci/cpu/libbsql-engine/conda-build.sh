#!/bin/bash

echo "CMD: conda build ${CONDA_CH} -c conda-forge -c defaults  conda/recipes/libbsql-engine/"
conda build ${CONDA_CH} -c conda-forge -c defaults  conda/recipes/libbsql-engine/
