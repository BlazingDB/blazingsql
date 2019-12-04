#!/bin/bash

set -e

echo '### Building BlazingSQL ###'

INSTALL_PREFIX=${INSTALL_PREFIX:=${PREFIX:=${CONDA_PREFIX}}}

echo '### Building bsql-io ###'
cd io
./conda/recipes/bsql-io/build.sh Release $INSTALL_PREFIX

echo '### Building bsql-comms ###'
cd ../comms
./conda/recipes/bsql-comms/build.sh Release $INSTALL_PREFIX

echo '### Building bsql-engine ###'
cd ../engine
./conda/recipes/libbsql-engine/build.sh Release $INSTALL_PREFIX
./conda/recipes/bsql-engine/build.sh $INSTALL_PREFIX

echo '### Building blazingsql ###'
cd ../pyblazing
./conda/recipes/pyblazing/build.sh

echo '### Building bsql-algebra ###'
cd ../algebra
./conda/recipes/bsql-algebra/build.sh $INSTALL_PREFIX
