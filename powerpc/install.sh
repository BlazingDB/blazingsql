#!/bin/bash
# usage: /new-path/python/environment/
set -e

VIRTUAL_ENV=$1

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
