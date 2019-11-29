#!/bin/bash
build_type="Release"
if [ ! -z $1 ]; then
  build_type=$1
  if [ "$build_type" != "Debug" ]; then
    build_type="Release"
  fi
fi

if [ -z ${2+x} ]
then
   INSTALL_PREFIX=$CONDA_PREFIX
else
   INSTALL_PREFIX=$2
fi

if [ ! -d "build" ]; then
  mkdir build
fi
cd build
cmake .. -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
make -j install
