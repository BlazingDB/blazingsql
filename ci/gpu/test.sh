NUMARGS=$#
ARGS=$*

if [ -n $CONDA_PREFIX ]; then
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREFIX/lib
fi

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

# NOTE: ensure all dir changes are relative to the location of this
# script, and that this script resides in the repo dir!
REPODIR=$(cd $(dirname $0); pwd)

VALIDARGS="io comms libengine pyblazing algebra -t -v -h -c e2e-tests"
HELP="$0 [-v] [-h] [-t] [-c] [e2e_test=\"test1,test2,...,testn\"]
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
   e2e-tests=   - Optional argument to use after 'pyblazing' run specific e2e
                  test groups.
                  The comma separated values are the e2e tests to run, where
                  each value is the python filename of the test located in
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
        if [ $TARGET_E2E_TEST_GROUPS == $a ] ; then
            echo "Invalid option: ${a}"
            exit 1
        fi
        continue
    fi
    if ! (echo " ${VALIDARGS} " | grep -q " ${a} "); then
        echo "Invalid option: ${a}"
        exit 1
    fi
    done
fi

# NOTE if WORKSPACE is not defined we assume the user is in the blazingsql project root folder
if [ -z $WORKSPACE ] ; then
    logger "WORKSPACE is not defined, it should point to the blazingsql project root folder"
    logger "Using $PWD as WORKSPACE"
    WORKSPACE=$PWD
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

        # If we are running on a GPUCI environment then force to set nrals to 1
        if [ "$BLAZINGSQL_E2E_IN_GPUCI_ENV" == "true" ] ; then
            logger "Running end to end tests SINGLE NODE (nrals=1) ..."
            export BLAZINGSQL_E2E_N_RALS=1
        fi

        cd ${WORKSPACE}/pyblazing/blazingsql/tests/BlazingSQLTest/
        SECONDS=0
        if [ "$E2E_TEST_GROUP" == "" ]; then
            python -m EndToEndTests.allE2ETest
        else
            python -m EndToEndTests.$E2E_TEST_GROUP
        fi
        duration=$SECONDS
        logger "Total time for end to end tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"

        # If we are running on a GPUCI environment then print final status for nrals=1
        if [ "$BLAZINGSQL_E2E_IN_GPUCI_ENV" == "true" ] ; then
            logger "End to end tests SINGLE NODE (nrals=1) ... DONE!"
        fi

        # If we are running on a GPUCI environment then run again the e2e but with nrals=2
        if [ "$BLAZINGSQL_E2E_IN_GPUCI_ENV" == "true" ] ; then
            logger "Running end to end tests DISTRIBUTED (nrals=2) ..."
            export BLAZINGSQL_E2E_N_RALS=2
            cd ${WORKSPACE}/pyblazing/blazingsql/tests/BlazingSQLTest/
            SECONDS=0
            if [ "$E2E_TEST_GROUP" == "" ]; then
                python -m EndToEndTests.allE2ETest
            else
                python -m EndToEndTests.$E2E_TEST_GROUP
            fi
            duration=$SECONDS
            logger "Total time for end to end tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
            logger "End to end tests DISTRIBUTED (nrals=2) ... DONE!"
        fi
    fi
fi
