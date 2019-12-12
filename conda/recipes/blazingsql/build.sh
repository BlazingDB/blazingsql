#!/bin/bash
# usage: ./build.sh build_type test
# example: ./build.sh Release|Debug ON|OFF

set -e

echo '### Building BlazingSQL ###'
INSTALL_PREFIX=${INSTALL_PREFIX:=${PREFIX:=${CONDA_PREFIX}}}

build_type="Release"
if [ ! -z $1 ]; then
  build_type=$1
fi

run_test="ON"
if [ ! -z $2 ]; then
  run_test=$2
fi

echo "install_prefix: "$INSTALL_PREFIX
echo "build_type: "$build_type
echo "run_test: "$run_test

echo '### Building bsql-io ###'
cd io
./conda/recipes/bsql-io/build.sh $INSTALL_PREFIX $build_type

echo '### Building bsql-comms ###'
cd ../comms
./conda/recipes/bsql-comms/build.sh $INSTALL_PREFIX $build_type

echo '### Building bsql-engine ###'
cd ../engine
./conda/recipes/libbsql-engine/build.sh $INSTALL_PREFIX $build_type $run_test
./conda/recipes/bsql-engine/build.sh $INSTALL_PREFIX

echo '### Building blazingsql ###'
cd ../pyblazing
./conda/recipes/pyblazing/build.sh

echo '### Building bsql-algebra ###'
cd ../algebra
./conda/recipes/bsql-algebra/build.sh $INSTALL_PREFIX $run_test

