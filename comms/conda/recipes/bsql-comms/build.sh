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

mkdir -p build
cd build

cmake .. -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
make -j install
