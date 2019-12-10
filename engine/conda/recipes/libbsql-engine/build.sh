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
   INSTALL_PREFIX=$1
fi

mkdir -p build
cd build

export CXXFLAGS="-L$INSTALL_PREFIX/lib"
export CFLAGS=$CXXFLAGS

echo "cmake -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=$build_type .."
cmake -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_EXE_LINKER_FLAGS="$CXXFLAGS" ..

echo "make -j blazingsql-engine"
make -j blazingsql-engine

echo "cp libblazingsql-engine.so ${INSTALL_PREFIX}/lib/libblazingsql-engine.so"
cp libblazingsql-engine.so ${INSTALL_PREFIX}/lib/libblazingsql-engine.so
