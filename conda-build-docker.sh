#!/bin/bash

# ================================================================
# NOTE
# cpu-build:
# - here we run conda build inside the docker container
# - builds the artifacts
# - but doesn't run any test
# - it just needs CPU
# - and it runs on top on gpuci/rapidsai-driver docker image
# - https://gpuci.gpuopenanalytics.com/job/blazingsql/job/gpuci/job/pyblazing/job/prb/job/pyblazing-cpu-build/
# ================================================================
# NOTE
# gpu-build:
# - here we install a new conda dev env inside the docker container
# - builds the artifacts
# - and run any all the tests: unit tests and e2e
# - it needs GPU
# - it runs on top on gpuci/rapidsai docker image
# - https://gpuci.gpuopenanalytics.com/job/blazingsql/job/gpuci/job/pyblazing/job/prb/job/pyblazing-gpu-build/
# ================================================================
# NOTE Examples:
# Run GPUCI jobs (first the gpu-build and then the cpu-build):
# ./conda-build-docker.sh cudf_version cuda_version python_version conda_token custom_label conda_username
#
# Run only the CPU BUILD job (use this one if you want to debug issues with conda build on gpuci)
# BLAZING_GPUCI_JOB=cpu-build ./conda-build-docker.sh cudf_version cuda_version python_version conda_token custom_label conda_username
#
# Run only the GPU BUILD job (use this one if you want to debug issues with tests on gpuci/gpu build)
# BLAZING_GPUCI_JOB=gpu-build ./conda-build-docker.sh cudf_version cuda_version python_version
#
# Run only the CPU BUILD job and upload the blazingsql package to your conda channel with the label main
# BLAZING_GPUCI_JOB=cpu-build ./conda-build-docker.sh 0.16 10.0 3.7 conda_token main conda_username
#
# Run GPUCI jobs with defaults:
# ./conda-build-docker.sh
# ================================================================
# NOTE Defaults:
# cudf_version=0.16
# cuda_version=10.0
# python_version=3.7
# conda_token=""
# custom_label=""
# conda_username="blazingsql-nightly"
# ================================================================
# NOTE Remarks:
# - In case a job fails then you will go automatically into the docker for debugging
# - Don't forget to kill all the containers after you finish
# ================================================================

NUMARGS=$#
ARGS=$*

VALIDARGS="-h"
HELP="# ================================================================
# NOTE
# cpu-build:
# - here we run conda build inside the docker container
# - builds the artifacts
# - but doesn't run any test
# - it just needs CPU
# - and it runs on top on gpuci/rapidsai-driver docker image
# - https://gpuci.gpuopenanalytics.com/job/blazingsql/job/gpuci/job/pyblazing/job/prb/job/pyblazing-cpu-build/
# ================================================================
# NOTE
# gpu-build:
# - here we install a new conda dev env inside the docker container
# - builds the artifacts
# - and run any all the tests: unit tests and e2e
# - it needs GPU
# - it runs on top on gpuci/rapidsai docker image
# - https://gpuci.gpuopenanalytics.com/job/blazingsql/job/gpuci/job/pyblazing/job/prb/job/pyblazing-gpu-build/
# ================================================================
# NOTE Examples:
# Run GPUCI jobs (first the gpu-build and then the cpu-build):
# ./conda-build-docker.sh cudf_version cuda_version python_version conda_token custom_label conda_username
#
# Run only the CPU BUILD job (use this one if you want to debug issues with conda build on gpuci)
# BLAZING_GPUCI_JOB=cpu-build ./conda-build-docker.sh cudf_version cuda_version python_version conda_token custom_label conda_username
#
# Run only the GPU BUILD job (use this one if you want to debug issues with tests on gpuci/gpu build)
# BLAZING_GPUCI_JOB=gpu-build ./conda-build-docker.sh cudf_version cuda_version python_version
#
# Run only the CPU BUILD job and upload the blazingsql package to your conda channel with the label main
# BLAZING_GPUCI_JOB=cpu-build ./conda-build-docker.sh 0.16 10.0 3.7 conda_token main conda_username
#
# Run GPUCI jobs with defaults:
# ./conda-build-docker.sh
# ================================================================
# NOTE Defaults:
# cudf_version=0.16
# cuda_version=10.0
# python_version=3.7
# conda_token=""
# custom_label=""
# conda_username="blazingsql-nightly"
# ================================================================
# NOTE Remarks:
# - In case a job fails then you will go automatically into the docker for debugging
# - Don't forget to kill all the containers after you finish
# ================================================================"

function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

if hasArg -h; then
    echo "${HELP}"
    exit 0
fi

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

export WORKSPACE=$PWD

if [ -z $BLAZING_GPUCI_JOB ]; then
    BLAZING_GPUCI_JOB=""
    echo "BLAZING_GPUCI_JOB: $BLAZING_GPUCI_JOB"
fi

if [ -z $BLAZING_GPUCI_OS ]; then
    BLAZING_GPUCI_OS="ubuntu16.04"
    echo "BLAZING_GPUCI_OS: $BLAZING_GPUCI_OS"
fi

CUDF_VERSION="0.16"
if [ ! -z $1 ]; then
    CUDF_VERSION=$1
fi
echo "CUDF_VERSION: $CUDF_VERSION"

CUDA_VERSION="10.0"
if [ ! -z $2 ]; then
    CUDA_VERSION=$2
fi
echo "CUDA_VERSION: $CUDA_VERSION"

PYTHON_VERSION="3.7"
if [ ! -z $3 ]; then
    PYTHON_VERSION=$3
fi
echo "PYTHON_VERSION: $PYTHON_VERSION"

if [ "$BLAZING_GPUCI_JOB" = "" ] || [ "$BLAZING_GPUCI_JOB" = "cpu-build" ]; then
    MY_UPLOAD_KEY=""
    UPLOAD_BLAZING="0"
    if [ ! -z $4 ]; then
        MY_UPLOAD_KEY=$4
        UPLOAD_BLAZING=1
    fi
    echo "MY_UPLOAD_KEY: $MY_UPLOAD_KEY"
    echo "UPLOAD_BLAZING: $UPLOAD_BLAZING"

    CUSTOM_LABEL=""
    if [ ! -z $5 ]; then
        CUSTOM_LABEL=$5
    fi
    echo "CUSTOM_LABEL: $CUSTOM_LABEL"

    CONDA_USERNAME="blazingsql-nightly"
    if [ ! -z $6 ]; then
        CONDA_USERNAME=$6
    fi
    echo "CONDA_USERNAME: $CONDA_USERNAME"
fi

if [ "$BLAZING_GPUCI_JOB" = "" ] || [ "$BLAZING_GPUCI_JOB" = "gpu-build" ]; then
    logger "Cleaning the workspace before start the GPU BUILD job ..."
    cd $WORKSPACE
    ./build.sh clean
    ./build.sh clean thirdparty

    gpu_build_cmd="./ci/gpu/build.sh"
    gpu_build_img=gpuci/rapidsai:$CUDF_VERSION-cuda${CUDA_VERSION}-devel-$BLAZING_GPUCI_OS-py$PYTHON_VERSION

    logger "Updating the docker image for the GPU BUILD job ..."
    echo "docker pull $gpu_build_img"
    docker pull $gpu_build_img

    gpu_container="blazingsql-gpuci-gpu-build-container"

    logger "Running the docker container for the GPU BUILD job ..."
    echo "docker run --name $gpu_container --rm -dti \
        --runtime=nvidia \
        -u $(id -u):$(id -g) \
        -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION \
        -e WORKSPACE=$WORKSPACE \
        -v /etc/passwd:/etc/passwd \
        -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
        $gpu_build_img \
        bash"
    docker run --name $gpu_container --rm -dti \
        --runtime=nvidia \
        -u $(id -u):$(id -g) \
        -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION \
        -e WORKSPACE=$WORKSPACE \
        -v /etc/passwd:/etc/passwd \
        -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
        $gpu_build_img \
        bash

    logger "Running the GPU BUILD job ..."
    echo "docker exec -ti $gpu_container $gpu_build_cmd"
    docker exec -ti $gpu_container $gpu_build_cmd

    if [ $? != 0 ]; then
        logger "Debugging the GPU BUILD job ... "
        echo "docker exec -ti $gpu_container bash"
        docker exec -ti $gpu_container bash
        #docker stop $gpu_container
        #docker rm $gpu_container
        #docker ps -a
    fi
fi

if [ "$BLAZING_GPUCI_JOB" = "" ] || [ "$BLAZING_GPUCI_JOB" = "cpu-build" ]; then
    logger "Cleaning the workspace before start the CPU BUILD job ..."
    cd $WORKSPACE
    ./build.sh clean
    ./build.sh clean thirdparty

    cpu_build_cmd="./ci/cpu/build.sh"
    cpu_build_img=gpuci/rapidsai-driver:$CUDF_VERSION-cuda${CUDA_VERSION}-devel-$BLAZING_GPUCI_OS-py$PYTHON_VERSION

    logger "Updating the docker image for the CPU BUILD job ..."
    echo "docker pull $cpu_build_img"
    docker pull $cpu_build_img

    cpu_container="blazingsql-gpuci-cpu-build-container"

    logger "Running the docker container for the CPU BUILD job ..."
    echo "docker run --name $cpu_container --rm -dti \
        -u $(id -u):$(id -g) \
        -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION \
        -e CONDA_USERNAME=$CONDA_USERNAME -e MY_UPLOAD_KEY=$MY_UPLOAD_KEY \
        -e UPLOAD_BLAZING=$UPLOAD_BLAZING -e CUSTOM_LABEL=$CUSTOM_LABEL \
        -e GIT_BRANCH="master" -e SOURCE_BRANCH="master" \
        -e WORKSPACE=$WORKSPACE \
        -v /etc/passwd:/etc/passwd \
        -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
        $cpu_build_img \
        bash"
    docker run --name $cpu_container --rm -dti \
        -u $(id -u):$(id -g) \
        -e CUDA_VER=${CUDA_VERSION} -e PYTHON=$PYTHON_VERSION \
        -e CONDA_USERNAME=$CONDA_USERNAME -e MY_UPLOAD_KEY=$MY_UPLOAD_KEY \
        -e UPLOAD_BLAZING=$UPLOAD_BLAZING -e CUSTOM_LABEL=$CUSTOM_LABEL \
        -e GIT_BRANCH="master" -e SOURCE_BRANCH="master" \
        -e WORKSPACE=$WORKSPACE \
        -v /etc/passwd:/etc/passwd \
        -v ${WORKSPACE}:${WORKSPACE} -w ${WORKSPACE} \
        $cpu_build_img \
        bash

    logger "Running the CPU BUILD job ..."
    echo "docker exec -ti $cpu_container $cpu_build_cmd"
    docker exec -ti $cpu_container $cpu_build_cmd

    if [ $? != 0 ]; then
        logger "Debugging the CPU BUILD job ... "
        echo "docker exec -ti $cpu_container bash"
        docker exec -ti $cpu_container bash
        #docker stop $cpu_container
        #docker rm $cpu_container
        #docker ps -a
    fi
fi

logger "You can run docker exec on the containers, if not needed then just kill them!"
docker ps -a

