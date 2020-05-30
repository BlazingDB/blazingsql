NUMARGS=$#
ARGS=$*

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

# NOTE: ensure all dir changes are relative to the location of this
# script, and that this script resides in the repo dir!
REPODIR=$(cd $(dirname $0); pwd)

VALIDARGS="io comms libengine pyblazing algebra -t -v -h -c e2e-tests"
HELP="$0 [-v] [-h] [-t] [-c] [e2e_test=]
   io           - test the IO C++ code only
   comms        - test the communications C++ code only
   libengine    - test the engine C++ code only
   pyblazing    - test the pyblazing Python package (end to end tests)
   algebra      - test the algebra package
   -t           - skip end to end tests (force not run pyblazing tests)
   -v           - verbose test mode
   -h           - print this text
   -c           - save and use a cache of the previous e2e test runs from
                  Google Docs. The cache (e2e-gspread-cache.parquet) will be
                  located inside the env BLAZINGSQL_E2E_LOG_DIRECTORY (which
                  is usually pointing to the CONDA_PREFIX dir)
   e2e-tests    - when use after 'pyblazing' run a specific e2e test
                  groups.
                  Comma separated of e2e tests, where each value is
                  the python filename of the test located in
                  blazingsql/pyblazing/blazingsql/tests/BlazingSQLTest/EndToEndTests/
                  (e.g. 'castTest, groupByTest' or 'literalTest' for single test).
                  Empty means will run all the e2e tests)
   default action (no args) is to test all code and packages
"

# Set defaults for vars modified by flags to this script
VERBOSE=""
QUIET="--quiet"
TESTS="ON"
GSPREAD_CACHE="false"
TARGET_E2E_TEST_GROUPS=""

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
    if [[ $a == *"="* ]]; then
        TARGET_E2E_TEST_GROUPS=${a#"e2e-tests="}
        continue
    fi
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
if hasArg -c; then
    GSPREAD_CACHE="true"
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
        blazingsql_testing_files_dir=$(realpath $BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY/../)
        logger "Using $blazingsql_testing_files_dir folder for end to end tests..."
        cd $blazingsql_testing_files_dir
        git pull
    fi

    if testAll || hasArg pyblazing; then
        # Abort script on first error
        set -e

        export BLAZINGSQL_E2E_GSPREAD_CACHE=$GSPREAD_CACHE
        export BLAZINGSQL_E2E_TARGET_TEST_GROUPS=$TARGET_E2E_TEST_GROUPS

        logger "Running end to end tests..."
        cd ${WORKSPACE}/pyblazing/blazingsql/tests/BlazingSQLTest/
        SECONDS=0
        if [ "$E2E_TEST_GROUP" == "" ]; then
            python -m EndToEndTests.allE2ETest
        else
            python -m EndToEndTests.$E2E_TEST_GROUP
        fi
        duration=$SECONDS
        echo "Total time for end to end tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
    fi
fi
