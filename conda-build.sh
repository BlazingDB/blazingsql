#!/bin/bash
# usage:  ./conda-build.sh cuda_version python_version
# example ./conda-build.sh 9.2|10.0 3.6|3/7

export GIT_DESCRIBE_TAG=`git describe --abbrev=0 --tags`
export GIT_DESCRIBE_NUMBER=`git rev-list ${GIT_DESCRIBE_TAG}..HEAD --count`

conda build -c conda-forge -c defaults -c rapidsai -c blazingsql/label/main --python=$2 --output-folder $PWD/cuda$1_py$2/ $PWD/conda/recipes/pyBlazing/
