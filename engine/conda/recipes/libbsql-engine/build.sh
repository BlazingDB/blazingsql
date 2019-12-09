#!/bin/bash
# usage: ./build.sh prefix build_type test
# example: ./build.sh $CONDA_PREFIX Release|Debug ON|OFF

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

export CXXFLAGS="-L$INSTALL_PREFIX/lib"
export CFLAGS=$CXXFLAGS

echo "CMD: cmake -DBUILD_TESTING=$run_test -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_EXE_LINKER_FLAGS=\"$CXXFLAGS\" .."
cmake -DBUILD_TESTING=$run_test -DCMAKE_BUILD_TYPE=$build_type -DCMAKE_EXE_LINKER_FLAGS="$CXXFLAGS" ..

echo "make -j blazingsql-engine"
if [ "$run_test" == "ON" ]; then
  make -j all
  ctest
else
  make -j blazingsql-engine
fi

echo "cp libblazingsql-engine.so ${INSTALL_PREFIX}/lib/libblazingsql-engine.so"
cp libblazingsql-engine.so ${INSTALL_PREFIX}/lib/libblazingsql-engine.so
