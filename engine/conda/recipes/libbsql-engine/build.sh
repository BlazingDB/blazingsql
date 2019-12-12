#!/bin/bash
# usage: ./build.sh build_type test prefix
# example: ./build.sh Release|Debug ON|OFF $CONDA_PREFIX

build_type="Release"
if [ ! -z $1 ]; then
  build_type=$1
fi

run_test="ON"
if [ ! -z $2 ]; then
  run_test=$2
fi

INSTALL_PREFIX=$CONDA_PREFIX
if [ ! -z $3 ]; then
   INSTALL_PREFIX=$3
fi

mkdir -p build
cd build

export LD_LIBRARY_PATH=$INSTALL_PREFIX/lib
export CXXFLAGS="-L$INSTALL_PREFIX/lib"
export CFLAGS=$CXXFLAGS

echo "CMD: cmake -DBUILD_TESTING=$run_test -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_EXE_LINKER_FLAGS=\"$CXXFLAGS\" .."
cmake -DBUILD_TESTING=$run_test -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_EXE_LINKER_FLAGS="$CXXFLAGS" ..

if [ "$run_test" == "ON" ]; then
  echo "make -j all && ctest"
  make -j all && ctest
else
  echo "make -j blazingsql-engine"
  make -j blazingsql-engine
fi

echo "cp libblazingsql-engine.so ${INSTALL_PREFIX}/lib/libblazingsql-engine.so"
cp libblazingsql-engine.so ${INSTALL_PREFIX}/lib/libblazingsql-engine.so
