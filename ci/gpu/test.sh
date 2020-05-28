NUMARGS=$#
ARGS=$*

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

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
   -t           - skip end to end tests (force not run pyblazing tests)
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
    SECONDS=0
    ctest
    duration=$SECONDS
    echo "Total time for IO Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if testAll || hasArg comms; then
    logger "Running Comm Unit tests..."
    cd ${WORKSPACE}/comms/build
    SECONDS=0
    ctest
    duration=$SECONDS
    echo "Total time for Comm Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if testAll || hasArg libengine; then
    logger "Running Engine Unit tests..."
    cd ${WORKSPACE}/engine/build
    SECONDS=0
    ctest
    duration=$SECONDS
    echo "Total time for Engine Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if testAll || hasArg algebra; then
    # TODO mario
    echo "TODO"
fi

if [ "$TESTS" == "OFF" ]; then
    logger "Skipping end to end tests..."
else
    if [ -z $BLAZINGSQL_E2E_DATA_DIRECTORY ] || [ -z $BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY ]; then
        if [ -d $CONDA_PREFIX/blazingsql-testing-files/data/ ]; then
            logger "Using $CONDA_PREFIX/blazingsql-testing-files folder for end to end tests..."
            cd $CONDA_PREFIX
            cd blazingsql-testing-files
            git pull
        else
            logger "Preparing $CONDA_PREFIX/blazingsql-testing-files folder for end to end tests..."
            cd $CONDA_PREFIX
            git clone --depth 1 https://github.com/BlazingDB/blazingsql-testing-files.git --branch master --single-branch
            cd blazingsql-testing-files
            tar xf data.tar.gz
            logger "$CONDA_PREFIX/blazingsql-testing-files folder for end to end tests... ready!"
        fi
        export BLAZINGSQL_E2E_DATA_DIRECTORY=$CONDA_PREFIX/blazingsql-testing-files/data/
        export BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY=$CONDA_PREFIX/blazingsql-testing-files/results/
    else
        blazingsql_testing_files_dir=$(realpath $BLAZINGSQL_E2E_DATA_DIRECTORY/../)
        logger "Using $blazingsql_testing_files_dir folder for end to end tests..."
        cd $blazingsql_testing_files_dir
        git pull
    fi

    if testAll || hasArg pyblazing; then
        # Abort script on first error
        set -e

        logger "Running end to end tests..."
        cd ${WORKSPACE}/pyblazing/blazingsql/tests/BlazingSQLTest/
        SECONDS=0
        python -m EndToEndTests.allE2ETest
        duration=$SECONDS
        echo "Total time for end to end tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
    fi
fi
