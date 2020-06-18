#!/bin/bash

# Copyright (c) 2019, NVIDIA CORPORATION.

# BlazingSQL build script

# This script is used to build the component(s) in this repo from
# source, and can be called with various options to customize the
# build as needed (see the help output for details)

# Abort script on first error
set -e

NUMARGS=$#
ARGS=$*

# NOTE: ensure all dir changes are relative to the location of this
# script, and that this script resides in the repo dir!
REPODIR=$(cd $(dirname $0); pwd)

VALIDARGS="clean thirdparty io comms libengine engine pyblazing algebra -t -v -g -n -h"
HELP="$0 [-v] [-g] [-n] [-h] [-t]
   clean        - remove all existing build artifacts and configuration (start
                  over) Use 'clean thirdparty' to delete thirdparty folder
   thirdparty   - build the Thirdparty C++ code only
   io           - build the IO C++ code only
   comms        - build the communications C++ code only
   libengine    - build the engine C++ code only
   engine       - build the engine Python package
   pyblazing    - build the pyblazing Python package
   algebra      - build the algebra Python package
   -t           - skip tests
   -v           - verbose build mode
   -g           - build for debug
   -n           - no install step
   -h           - print this text
   default action (no args) is to build and install all code and packages
"

THIRDPARTY_BUILD_DIR=${REPODIR}/thirdparty/aws-cpp/build
IO_BUILD_DIR=${REPODIR}/io/build
COMMS_BUILD_DIR=${REPODIR}/comms/build
LIBENGINE_BUILD_DIR=${REPODIR}/engine/build
ENGINE_BUILD_DIR=${REPODIR}/engine
PYBLAZING_BUILD_DIR=${REPODIR}/pyblazing
ALGEBRA_BUILD_DIR=${REPODIR}/algebra
BUILD_DIRS="${THIRDPARTY_BUILD_DIR} ${IO_BUILD_DIR} ${COMMS_BUILD_DIR} ${LIBENGINE_BUILD_DIR}"

# Set defaults for vars modified by flags to this script
VERBOSE=""
QUIET="--quiet"
BUILD_TYPE=RelWithDebInfo
INSTALL_TARGET=install
TESTS="ON"

# Set defaults for vars that may not have been defined externally
#  FIXME: if INSTALL_PREFIX is not set, check PREFIX, then check
#         CONDA_PREFIX, but there is no fallback from there!
INSTALL_PREFIX=${INSTALL_PREFIX:=${PREFIX:=${CONDA_PREFIX}}}
PARALLEL_LEVEL=${PARALLEL_LEVEL:=""}
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$INSTALL_PREFIX/lib
export CXXFLAGS="-L$INSTALL_PREFIX/lib"
export CFLAGS=$CXXFLAGS
export CUDACXX=/usr/local/cuda/bin/nvcc

function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

function buildAll {
    ((${NUMARGS} == 0 )) || !(echo " ${ARGS} " | grep -q " [^-]\+ ")
}

if hasArg -h; then
    echo "${HELP}"
    exit 0
fi

# Check for valid usage
if (( ${NUMARGS} != 0 )); then
    for a in ${ARGS}; do
    if ! (echo " ${VALIDARGS} " | grep -q " ${a} "); then
        echo "Invalid option: ${a}"
        exit 1
    fi
    done
fi

# Process flags
if hasArg -v; then
    VERBOSE=1
    QUIET=""
fi
if hasArg -g; then
    BUILD_TYPE=Debug
fi
if hasArg -n; then
    INSTALL_TARGET=""
fi
if hasArg -t; then
    TESTS="OFF"
fi

# If clean given, run it prior to any other steps
if hasArg clean; then
    # If the dirs to clean are mounted dirs in a container, the
    # contents should be removed but the mounted dirs will remain.
    # The find removes all contents but leaves the dirs, the rmdir
    # attempts to remove the dirs but can fail safely.
    for bd in ${BUILD_DIRS}; do
    if [ -d ${bd} ]; then
        find ${bd} -mindepth 1 -delete
        rmdir ${bd} || true
    fi
    done

    if hasArg thirdparty; then
        rm -rf ${REPODIR}/thirdparty/rapids/
        rm -rf ${REPODIR}/thirdparty/aws-cpp/
    fi

    exit 0
fi

################################################################################

if buildAll || hasArg io || hasArg libengine || hasArg thirdparty; then
    if [ ! -d "${REPODIR}/thirdparty/rapids/" ]; then
        cd ${REPODIR}/thirdparty/
        DIR_BSQL="bsql-rapids-thirdparty"
        mkdir -p ${INSTALL_PREFIX}/include/$DIR_BSQL/cub
        mkdir -p ${INSTALL_PREFIX}/include/$DIR_BSQL/libcudacxx/libcxx/include
        mkdir -p ${REPODIR}/thirdparty/rapids/
        cd ${REPODIR}/thirdparty/rapids/

        cudf_version=$(conda list | grep libcudf|tail -n 1|awk '{print $2}')
        cudf_version="$(cut -d '.' -f 1 <<< "$cudf_version")"."$(cut -d '.' -f 2 <<< "$cudf_version")"
        cudf_version="0.14" # TODO william jp mario we need to use 0.15 here
        echo "cudf_version for rapids 3rdparty is: $cudf_version"

# CODE FOR 0.15
#         git clone -b branch-$cudf_version --recurse-submodules https://github.com/rapidsai/cudf.git
#         cd cudf/cpp
#         mkdir build
#         cd build
#         cmake -DBUILD_TESTS=OFF ..
#         cp -rf _deps/cub-src/cub/* ${INSTALL_PREFIX}/include/$DIR_BSQL/cub
#         cp -rf _deps/libcudacxx-src/include/ ${INSTALL_PREFIX}/include/$DIR_BSQL/libcudacxx
#         cp -rf _deps/libcudacxx-src/libcxx/include/* ${INSTALL_PREFIX}/include/$DIR_BSQL/libcudacxx/libcxx/include


        DIR_BSQL="bsql-rapids-thirdparty"
        mkdir -p $INSTALL_PREFIX/include/bsql-rapids-thirdparty/
        cd ${REPODIR}/thirdparty/rapids/
        git clone -b branch-0.14 --recurse-submodules https://github.com/rapidsai/cudf.git

        cp -rf cudf/thirdparty/cub/* ${INSTALL_PREFIX}/include/$DIR_BSQL/cub

        rm -rf cudf/thirdparty/libcudacxx/libcxx/test/
        cp -rf cudf/thirdparty/libcudacxx/* ${INSTALL_PREFIX}/include/$DIR_BSQL/libcudacxx

        if [[ $CONDA_BUILD -eq 1 ]]; then
            cd ${REPODIR}
            # WARNING DO NOT TOUCH OR CHANGE THESE PATHS (william mario c.gonzales)
            echo "==>> In conda build env: thirdparty/rapids headers"
            echo "==>> Current working directory: $PWD"
            cp --remove-destination -rfu /conda/conda-bld/blazingsql_*/_h_env*/include/$DIR_BSQL /conda/conda-bld/blazingsql_*/_build_env/include
        fi

        echo "thirdparty/rapids headers has been installed in ${INSTALL_PREFIX}/include/$DIR_BSQL"
    else
        echo "thirdparty/rapids is already installed in ${INSTALL_PREFIX}/include/$DIR_BSQL"
    fi

    if [ ! -d "${REPODIR}/thirdparty/aws-cpp/" ]; then
        cd ${REPODIR}/thirdparty/
        aws_cpp_version=$(conda list | grep aws-sdk-cpp|tail -n 1|awk '{print $2}')
        echo "aws_cpp_version for aws cpp sdk 3rdparty is: $aws_cpp_version"

        git clone -b $aws_cpp_version --depth=1 https://github.com/aws/aws-sdk-cpp.git ${REPODIR}/thirdparty/aws-cpp/
        mkdir -p ${THIRDPARTY_BUILD_DIR}
        cd ${THIRDPARTY_BUILD_DIR}
        cmake -GNinja \
            -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
            -DCMAKE_INSTALL_LIBDIR=lib \
            -DBUILD_ONLY='s3-encryption' \
            -DENABLE_UNITY_BUILD=on \
            -DENABLE_TESTING=off \
            -DCMAKE_BUILD_TYPE=Release \
            ..
        ninja install

        if [[ $CONDA_BUILD -eq 1 ]]; then
            cd ${REPODIR}
            # WARNING DO NOT TOUCH OR CHANGE THESE PATHS (william mario c.gonzales)
            echo "==>> In conda build env: aws sdk cpp thirdparty"
            echo "==>> Current working directory: $PWD"
            cp --remove-destination -rfu /conda/conda-bld/blazingsql_*/_h_env*/include/aws/* /conda/conda-bld/blazingsql_*/_build_env/include/aws/
            cp --remove-destination -rfu /conda/conda-bld/blazingsql_*/_h_env*/lib/*aws* /conda/conda-bld/blazingsql_*/_build_env/lib/
            cp --remove-destination -rfu /conda/conda-bld/blazingsql_*/_h_env*/lib/cmake/*aws* /conda/conda-bld/blazingsql_*/_build_env/lib/cmake/
            cp --remove-destination -rfu /conda/conda-bld/blazingsql_*/_h_env*/lib/cmake/AWS* /conda/conda-bld/blazingsql_*/_build_env/lib/cmake/
            cp --remove-destination -rfu /conda/conda-bld/blazingsql_*/_h_env*/lib/cmake/*Aws* /conda/conda-bld/blazingsql_*/_build_env/lib/cmake/
            cp --remove-destination -rfu /conda/conda-bld/blazingsql_*/_h_env*/lib/pkgconfig/*aws* /conda/conda-bld/blazingsql_*/_build_env/lib/pkgconfig/
        fi
    else
        echo "thirdparty/aws-cpp/ is already installed in ${INSTALL_PREFIX}"
    fi
fi

################################################################################

if buildAll || hasArg io; then

    mkdir -p ${IO_BUILD_DIR}
    cd ${IO_BUILD_DIR}
    cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
          -DBUILD_TESTING=${TESTS} \
          -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ..

    if [[ ${TESTS} == "ON" ]]; then
        make -j${PARALLEL_LEVEL} all
    else
        make -j${PARALLEL_LEVEL} VERBOSE=${VERBOSE}
    fi

    if [[ ${INSTALL_TARGET} != "" ]]; then
        make -j${PARALLEL_LEVEL} install VERBOSE=${VERBOSE}
    fi
fi

if buildAll || hasArg comms; then

    mkdir -p ${COMMS_BUILD_DIR}
    cd ${COMMS_BUILD_DIR}
    cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
          -DBUILD_TESTING=${TESTS} \
          -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ..

    if [[ ${TESTS} == "ON" ]]; then
        make -j${PARALLEL_LEVEL} all
    else
        make -j${PARALLEL_LEVEL} VERBOSE=${VERBOSE}
    fi

    if [[ ${INSTALL_TARGET} != "" ]]; then
        make -j${PARALLEL_LEVEL} install VERBOSE=${VERBOSE}
    fi
fi

if buildAll || hasArg libengine; then
    echo "Building libengine"
    mkdir -p ${LIBENGINE_BUILD_DIR}
    cd ${LIBENGINE_BUILD_DIR}
    echo "cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} -DBUILD_TESTING=${TESTS} -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_EXE_LINKER_FLAGS=$CXXFLAGS .."
    cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
          -DBUILD_TESTING=${TESTS} \
          -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
          -DCMAKE_EXE_LINKER_FLAGS="$CXXFLAGS" ..

    echo "Building libengine: make step"
    if [[ ${TESTS} == "ON" ]]; then
        echo "make -j4 all"
        make -j4 all
    else
        echo "make -j4 blazingsql-engine VERBOSE=${VERBOSE}"
        make -j4 blazingsql-engine VERBOSE=${VERBOSE}
    fi

    if [[ ${INSTALL_TARGET} != "" ]]; then
        echo "make -j4 install VERBOSE=${VERBOSE}"
        make -j4 install VERBOSE=${VERBOSE}
        cp libblazingsql-engine.so ${INSTALL_PREFIX}/lib/libblazingsql-engine.so
    fi
fi

if buildAll || hasArg engine; then
    echo "Building engine (cython wrapper)"
    cd ${ENGINE_BUILD_DIR}
    rm -f ./bsql_engine/io/io.h
    rm -f ./bsql_engine/io/io.cpp

    if [[ ${INSTALL_TARGET} != "" ]]; then
        python setup.py build_ext --inplace
        if [ $? != 0 ]; then
            exit 1
        fi
        python setup.py install --single-version-externally-managed --record=record.txt
        if [ $? != 0 ]; then
            exit 1
        fi

        if [[ $CONDA_BUILD -eq 1 ]]; then
            cp `pwd`/cio*.so `pwd`/../../_h_env*/lib/python*/site-packages
            cp -r `pwd`/bsql_engine `pwd`/../../_h_env*/lib/python*/site-packages
        fi
    else
        python setup.py build_ext --inplace --library-dir=${LIBENGINE_BUILD_DIR}
        if [ $? != 0 ]; then
            exit 1
        fi
    fi
fi

if buildAll || hasArg pyblazing; then

    cd ${PYBLAZING_BUILD_DIR}
    if [[ ${INSTALL_TARGET} != "" ]]; then
        python setup.py build_ext --inplace
        if [ $? != 0 ]; then
            exit 1
        fi
        python setup.py install --single-version-externally-managed --record=record.txt
        if [ $? != 0 ]; then
            exit 1
        fi

        if [[ $CONDA_BUILD -eq 1 ]]; then
            cp -r `pwd`/pyblazing `pwd`/../../_h_env*/lib/python*/site-packages
            cp -r `pwd`/blazingsql `pwd`/../../_h_env*/lib/python*/site-packages
        fi
    else
        python setup.py build_ext --inplace
        if [ $? != 0 ]; then
            exit 1
        fi
    fi
fi

if buildAll || hasArg algebra; then

    cd ${ALGEBRA_BUILD_DIR}
    if [[ ${TESTS} == "ON" ]]; then
        mvn clean install -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/ $QUIET
    else
        mvn clean install -Dmaven.test.skip=true -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/ $QUIET
    fi

    if [[ ${INSTALL_TARGET} != "" ]]; then
        cp blazingdb-calcite-application/target/BlazingCalcite.jar $INSTALL_PREFIX/lib/blazingsql-algebra.jar
        cp blazingdb-calcite-core/target/blazingdb-calcite-core.jar $INSTALL_PREFIX/lib/blazingsql-algebra-core.jar
    fi
fi

