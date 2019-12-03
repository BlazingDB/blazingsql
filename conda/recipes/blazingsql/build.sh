#!/bin/bash

set -e

echo '### Building BlazingSQL ###'

INSTALL_PREFIX=${INSTALL_PREFIX:=${PREFIX:=${CONDA_PREFIX}}}

cd io
./conda/recipes/bsql-io/build.sh Release $INSTALL_PREFIX

cd ../comms
./conda/recipes/bsql-comms/build.sh Release $INSTALL_PREFIX

cd ../engine
./conda/recipes/libbsql-engine/build.sh Release $INSTALL_PREFIX
./conda/recipes/bsql-engine/build.sh $INSTALL_PREFIX

cd ../pyblazing
./conda/recipes/pyblazing/build.sh

cd ../algebra
./conda/recipes/bsql-algebra/build.sh $INSTALL_PREFIX
