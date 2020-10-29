#!/bin/bash
# usage: /new-path/python/environment/
# NOTE: this install script is for installing on Summit Supercomputer
set -e

NUMARGS=$#
if (( ${NUMARGS} != 1 )); then
  echo "Only one argument expected which should be environment prefix path"
  exit 1
fi

VIRTUAL_ENV=$1

if [ ! -d $VIRTUAL_ENV ]; then
    echo "The environment prefix path does not exist"
    exit 1
fi

echo "### Modules loading ###"
module load gcc/7.4.0
module load python/3.7.0
module load cmake/3.17.3
module load boost/1.66.0
module load cuda/10.1.243
module load zlib
module load texinfo
module load openblas
module load netlib-lapack
# for UCX BEGIN
module load hwloc
module load gdrcopy
# for UCX END
module list

echo "### Modules loading ###"
python -m venv $VIRTUAL_ENV
source $VIRTUAL_ENV/bin/activate

echo "### Building ###"
./build.sh $VIRTUAL_ENV
