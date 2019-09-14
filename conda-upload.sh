#!/bin/bash
# usage: ./conda-upload.sh cuda_version python_version token
# usage: ./conda-upload.sh 9.2|10.0 3.6|3.7 123

export GIT_DESCRIBE_TAG=`git describe --abbrev=0 --tags`
export GIT_DESCRIBE_NUMBER=`git rev-list ${GIT_DESCRIBE_TAG}..HEAD --count`
export PATH_FILE=`conda build --python=$2 conda/recipes/pyBlazing/ --output-folder $PWD/cuda$1_py$2/ --output`

anaconda -t $3 upload --user blazingsql --label main --label cuda$1 $PATH_FILE --force
