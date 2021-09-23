#!/bin/bash

NUMARGS=$#
ARGS=$*

# Abort script on first error
set -e

if [ -n $CONDA_PREFIX ]; then
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREFIX/lib:$CONDA_PREFIX/lib64
fi

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

# NOTE: ensure all dir changes are relative to the location of this
# script, and that this script resides in the repo dir!
REPODIR=$(cd $(dirname $0); pwd)

# TODO william kharoly felipe we should try to enable and use this param in the future (compare result from spreadsheet): add -c
VALIDARGS="io libengine algebra pyblazing e2e -t -v -h tests"
HELP="$0 [-v] [-h] [-t] [-c] [e2e_test=\"test1,test2,...,testn\"]
   io           - test the IO C++ code only
   libengine    - test the engine C++ code only
   algebra      - test the algebra package
   pyblazing    - test the pyblazing interface
   e2e          - test the end to end tests
   -t           - skip end to end tests (force not run 'e2e' tests)
   -v           - verbose test mode
   -h           - print this text
   tests=       - Optional argument to use after 'e2e' run specific e2e
                  test groups.
                  The comma separated values are the e2e tests to run, where
                  each value is the python filename of the test located in
                  blazingsql/tests/BlazingSQLTest/EndToEndTests/
                  (e.g. 'castTest, groupByTest' or 'literalTest' for single test).
                  Empty means will run all the e2e tests)
   default action (no args) is to test all code and packages
"

# TODO william kharoly felipe we should try to enable and use this param in the future (compare result from spreadsheet)
#    -c           - save and use a cache of the previous e2e test runs from
#                   Google Docs. The cache (e2e-gspread-cache.parquet) will be
#                   located inside the env BLAZINGSQL_E2E_LOG_DIRECTORY (which
#                   is usually pointing to the CONDA_PREFIX dir)

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
        TARGET_E2E_TEST_GROUPS=${a#"tests="}
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

# TODO william kharoly felipe we should try to enable and use this param in the future (compare result from spreadsheet)
#if hasArg -c; then
#    GSPREAD_CACHE="true"
#fi

################################################################################

if testAll || hasArg io; then
    logger "Running IO Unit tests..."
    cd ${WORKSPACE}/io/build
    SECONDS=0
    ctest --verbose
    duration=$SECONDS
    echo "Total time for IO Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if testAll || hasArg libengine; then
    logger "Running Engine Unit tests..."
    cd ${WORKSPACE}/engine/build
    SECONDS=0
    ctest --verbose
    duration=$SECONDS
    echo "Total time for Engine Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if testAll || hasArg algebra; then
    # TODO mario
    echo "TODO"
fi

if testAll || hasArg pyblazing; then
    logger "Running Pyblazing Unit tests..."
    SECONDS=0
    cd ${WORKSPACE}/pyblazing/tests
    pytest
    duration=$SECONDS
    echo "Total time for Pyblazing Unit tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
fi

if [ "$TESTS" == "OFF" ]; then
    logger "Skipping end to end tests..."
else
    if testAll || hasArg e2e; then
        if [ -z $BLAZINGSQL_E2E_DATA_DIRECTORY ] || [ -z $BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY ]; then
            if [ -d $CONDA_PREFIX/blazingsql-testing-files/data/ ]; then
                logger "Using $CONDA_PREFIX/blazingsql-testing-files folder for end to end tests..."
                cd $CONDA_PREFIX
                cd blazingsql-testing-files
                git pull
            else
                set +e
                logger "Preparing $CONDA_PREFIX/blazingsql-testing-files folder for end to end tests..."
                cd $CONDA_PREFIX

                # Only for PRs
                PR_BR="blazingdb:"${TARGET_BRANCH}
                if [ ! -z "${PR_AUTHOR}" ]; then
                    echo "PR_AUTHOR: "${PR_AUTHOR}
                    git clone --depth 1 https://github.com/${PR_AUTHOR}/blazingsql-testing-files.git --branch ${SOURCE_BRANCH} --single-branch
                    # if branch exits
                    if [ $? -eq 0 ]; then
                        echo "The fork exists"
                        PR_BR=${PR_AUTHOR}":"${SOURCE_BRANCH}
                    else
                        echo "The fork doesn't exist"
                        git clone --depth 1 https://github.com/rapidsai/blazingsql-testing-files.git --branch ${TARGET_BRANCH} --single-branch
                    fi
                fi
                set -e

                echo "Cloned from "${PR_BR}
                cd blazingsql-testing-files/data
                tar xf tpch.tar.gz
                tar xf tpch-with-nulls.tar.gz
                tar xf tpch-json.tar.gz -C .
                tar xf smiles.tar.gz
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

        # TODO william kharoly felipe we should try to enable and use this param in the future (compare result from spreadsheet)
        #export BLAZINGSQL_E2E_GSPREAD_CACHE=$GSPREAD_CACHE

        export BLAZINGSQL_E2E_TARGET_TEST_GROUPS=$TARGET_E2E_TEST_GROUPS

        for include_nulls in "false" "true"; do
            # If we are running on a GPUCI environment then force to set nrals to 1
            if [ "$BLAZINGSQL_E2E_IN_GPUCI_ENV" == "true" ] ; then
                logger "Running end to end tests SINGLE NODE (nrals=1), including nulls: $include_nulls ..."
                export BLAZINGSQL_E2E_N_RALS=1
                export BLAZINGSQL_E2E_TEST_WITH_NULLS=$include_nulls
            fi

            cd ${WORKSPACE}/tests/BlazingSQLTest/
            SECONDS=0

            python -m EndToEndTests.mainE2ETests --config-file config.yaml

            duration=$SECONDS
            logger "Total time for end to end tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"

            # If we are running on a GPUCI environment then print final status for nrals=1
            if [ "$BLAZINGSQL_E2E_IN_GPUCI_ENV" == "true" ] ; then
                logger "End to end tests SINGLE NODE (nrals=1), including nulls: $include_nulls ... DONE!"
            fi

            # If we are running on a GPUCI environment then run again the e2e but with nrals=2
            if [ "$BLAZINGSQL_E2E_IN_GPUCI_ENV" == "true" ] ; then
                logger "Running end to end tests DISTRIBUTED (nrals=2), including nulls: $include_nulls ..."
                export BLAZINGSQL_E2E_N_RALS=2
                export BLAZINGSQL_E2E_TEST_WITH_NULLS=$include_nulls
                cd ${WORKSPACE}/tests/BlazingSQLTest/
                SECONDS=0

                python -m EndToEndTests.mainE2ETests --config-file config.yaml

                duration=$SECONDS
                logger "Total time for end to end tests: $(($duration / 60)) minutes and $(($duration % 60)) seconds"
                logger "End to end tests DISTRIBUTED (nrals=2), including nulls: $include_nulls ... DONE!"
            fi
        done
    fi
fi
