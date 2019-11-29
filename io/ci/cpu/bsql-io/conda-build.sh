#!/bin/bash

echo "CMD: conda build ${CONDA_CH} -c conda-forge -c defaults conda/recipes/bsql-io/"
conda build ${CONDA_CH} -c conda-forge -c defaults conda/recipes/bsql-io/
