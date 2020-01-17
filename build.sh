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

VALIDARGS="clean io comms libengine engine pyblazing algebra -t -v -g -n -h"
HELP="$0 [-v] [-g] [-n] [-h] [-t]
   clean        - remove all existing build artifacts and configuration (start
                  over)
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

IO_BUILD_DIR=${REPODIR}/io/build
COMMS_BUILD_DIR=${REPODIR}/comms/build
LIBENGINE_BUILD_DIR=${REPODIR}/engine/build
ENGINE_BUILD_DIR=${REPODIR}/engine
PYBLAZING_BUILD_DIR=${REPODIR}/pyblazing
ALGEBRA_BUILD_DIR=${REPODIR}/algebra
BUILD_DIRS="${IO_BUILD_DIR} ${COMMS_BUILD_DIR} ${LIBENGINE_BUILD_DIR}"

# Set defaults for vars modified by flags to this script
VERBOSE=""
BUILD_TYPE=Release
INSTALL_TARGET=install
TESTS="ON"

# Set defaults for vars that may not have been defined externally
#  FIXME: if INSTALL_PREFIX is not set, check PREFIX, then check
#         CONDA_PREFIX, but there is no fallback from there!
INSTALL_PREFIX=${INSTALL_PREFIX:=${CONDA_PREFIX}}
PARALLEL_LEVEL=${PARALLEL_LEVEL:=""}

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
fi


################################################################################

if buildAll || hasArg io; then

    echo ">>>> mkdir -p ${IO_BUILD_DIR}"
    mkdir -p ${IO_BUILD_DIR}
    echo ">>>> cd ${IO_BUILD_DIR}"
    cd ${IO_BUILD_DIR}
    echo ">>>> cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} -DBUILD_TESTING=${TESTS} -DCMAKE_BUILD_TYPE=${BUILD_TYPE} .."
    cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
          -DBUILD_TESTING=${TESTS} \
          -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ..

    if [[ ${TESTS} == "ON" ]]; then
        echo ">>>> make -j${PARALLEL_LEVEL} all"
        make -j${PARALLEL_LEVEL} all
    else
        echo ">>>> make -j${PARALLEL_LEVEL} VERBOSE=${VERBOSE}"
        make -j${PARALLEL_LEVEL} VERBOSE=${VERBOSE}
    fi

    if [[ ${INSTALL_TARGET} != "" ]]; then
        echo ">>>> make -j${PARALLEL_LEVEL} install VERBOSE=${VERBOSE}"
        make -j${PARALLEL_LEVEL} install VERBOSE=${VERBOSE}
    fi
fi

if buildAll || hasArg comms; then

    echo ">>>> mkdir -p ${COMMS_BUILD_DIR}"
    mkdir -p ${COMMS_BUILD_DIR}
    echo ">>>> cd ${COMMS_BUILD_DIR}"
    cd ${COMMS_BUILD_DIR}
    echo ">>>> cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} -DBUILD_TESTING=${TESTS} -DCMAKE_BUILD_TYPE=${BUILD_TYPE} .."
    cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
          -DBUILD_TESTING=${TESTS} \
          -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ..

    if [[ ${TESTS} == "ON" ]]; then
        echo ">>>> make -j${PARALLEL_LEVEL} all"
        make -j${PARALLEL_LEVEL} all
    else
        echo ">>>> make -j${PARALLEL_LEVEL} VERBOSE=${VERBOSE}"
        make -j${PARALLEL_LEVEL} VERBOSE=${VERBOSE}
    fi

    if [[ ${INSTALL_TARGET} != "" ]]; then
        echo ">>>> make -j${PARALLEL_LEVEL} install VERBOSE=${VERBOSE}"
        make -j${PARALLEL_LEVEL} install VERBOSE=${VERBOSE}
    fi
fi

if buildAll || hasArg libengine; then

    echo ">>>> mkdir -p ${LIBENGINE_BUILD_DIR}"
    mkdir -p ${LIBENGINE_BUILD_DIR}
    echo ">>>> cd ${LIBENGINE_BUILD_DIR}"
    cd ${LIBENGINE_BUILD_DIR}
    echo ">>>> cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} -DBUILD_TESTING=${TESTS} -DCMAKE_BUILD_TYPE=${BUILD_TYPE} .."
    cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
          -DBUILD_TESTING=${TESTS} \
          -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ..

    if [[ ${TESTS} == "ON" ]]; then
        echo ">>>> make -j${PARALLEL_LEVEL} all"
        make -j${PARALLEL_LEVEL} all
    else
        echo ">>>> make -j${PARALLEL_LEVEL} blazingsql-engine VERBOSE=${VERBOSE}"
        make -j${PARALLEL_LEVEL} blazingsql-engine VERBOSE=${VERBOSE}
    fi

    if [[ ${INSTALL_TARGET} != "" ]]; then
        echo ">>>> make -j${PARALLEL_LEVEL} install VERBOSE=${VERBOSE}"
        make -j${PARALLEL_LEVEL} install VERBOSE=${VERBOSE}
    fi
fi

if buildAll || hasArg engine; then

    cd ${ENGINE_BUILD_DIR}
    if [[ ${INSTALL_TARGET} != "" ]]; then
        python setup.py build_ext --inplace
        python setup.py install --single-version-externally-managed --record=record.txt
    else
        python setup.py build_ext --inplace --library-dir=${LIBENGINE_BUILD_DIR}
    fi
   cp `pwd`/cio*.so `pwd`/../../_h_env*/lib/python*/site-packages
   cp -r `pwd`/bsql_engine `pwd`/../../_h_env*/lib/python*/site-packages
fi

if buildAll || hasArg pyblazing; then

    cd ${PYBLAZING_BUILD_DIR}
    if [[ ${INSTALL_TARGET} != "" ]]; then
        python setup.py build_ext --inplace
        python setup.py install --single-version-externally-managed --record=record.txt
    else
        python setup.py build_ext --inplace
    fi
fi

if buildAll || hasArg algebra; then

    cd ${ALGEBRA_BUILD_DIR}
    if [[ ${TESTS} == "ON" ]]; then
        mvn clean install -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/
    else
        mvn clean install -Dmaven.test.skip=true -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/
    fi
    if [[ ${INSTALL_TARGET} != "" ]]; then
        cp blazingdb-calcite-application/target/BlazingCalcite.jar $INSTALL_PREFIX/lib/blazingsql-algebra.jar
        cp blazingdb-calcite-core/target/blazingdb-calcite-core.jar $INSTALL_PREFIX/lib/blazingsql-algebra-core.jar
    fi
fi

