#!/bin/bash
# usage:  ./conda-build.sh cuda_version python_version build_number
# example ./conda-build.sh 10.0|9.2     3.7|3.6        123

export BUILD=$3
conda build -c conda-forge -c defaults --python=$2 --output-folder $PWD/cuda$1_py$2/ .
