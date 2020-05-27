NUMARGS=$#
ARGS=$*

# NOTE: ensure all dir changes are relative to the location of this
# script, and that this script resides in the repo dir!
REPODIR=$(cd $(dirname $0); pwd)

VALIDARGS="io comms libengine pyblazing algebra -t -v -h"
HELP="$0 [-v] [-h] [-t]
   io           - test the IO C++ code only
   comms        - test the communications C++ code only
   libengine    - test the engine C++ code only
   pyblazing    - test the pyblazing Python package (end to end tests)
   algebra      - test the algebra package
   -t           - skip end to end tests
   -v           - verbose test mode
   -h           - print this text
   default action (no args) is to test all code and packages
"

# Set defaults for vars modified by flags to this script
VERBOSE=""
QUIET="--quiet"
TESTS="ON"

function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

function testAll {
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

if testAll || hasArg io; then
    logger "Running IO Unit tests..."
    cd ${WORKSPACE}/io/build
    date
    ctest
    date
fi

if testAll || hasArg comms; then
    logger "Running Comm Unit tests..."
    cd ${WORKSPACE}/comms/build
    date
    ctest
    date
fi

if testAll || hasArg libengine; then
    logger "Running Engine Unit tests..."
    cd ${WORKSPACE}/engine/build
    date
    ctest
    date
fi

if testAll || hasArg algebra; then
    # TODO mario
    echo "TODO"
fi

if testAll || hasArg pyblazing; then
    logger "Running end to end tests..."
    cd ${WORKSPACE}/pyblazing/blazingsql/tests/BlazingSQLTest/
    date
    python -m EndToEndTests.allE2ETest
    date
fi
