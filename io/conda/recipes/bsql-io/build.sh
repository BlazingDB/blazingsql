#!/bin/bash
# usage: ./build.sh prefix build_type
# example: ./build.sh $CONDA_PREFIX Release|Debug

INSTALL_PREFIX=$CONDA_PREFIX
if [ ! -z $1 ]; then
   INSTALL_PREFIX=$1
fi

build_type="Release"
if [ ! -z $2 ]; then
  build_type=$2
fi

run_test="ON"
if [ ! -z $3 ]; then
  run_test=$3
fi

mkdir -p build
cd build

echo "cmake .. -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}"
cmake .. -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}

if [ "$run_test" == "ON" ]; then
  echo "make -j all && make -j install && ctest"
  make -j all && make -j install && ctest
else
  echo "make -j install"
  make -j install
fi
