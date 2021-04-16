#!/bin/bash
# usage: ./dependencies.sh rapids_version cuda_version nightly
# example: ./dependencies.sh 0.19 10.2
#          ./dependencies.sh 0.20 11.0 nightly

set -e

RAPIDS="0.19"
if [ ! -z $1 ]; then
    RAPIDS=$1
fi

CUDA="10.2"
if [ ! -z $2 ]; then
    CUDA=$2
fi

CHANNEL=""
if [ ! -z $3 ]; then
    CHANNEL="-nightly"
fi

echo "Install Dependencies"
conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.25 ninja mysql-connector-cpp=8.0.23 libpq=13 nlohmann_json
# NOTE cython must be the same of cudf (for 0.11 and 0.12 cython is >=0.29,<0.30)
conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive pytest tqdm ipywidgets

echo "Install RAPIDS dependencies"
conda install --yes -c rapidsai$CHANNEL -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS dask-cudf=$RAPIDS cudf=$RAPIDS ucx-py=$RAPIDS ucx-proc=*=gpu cudatoolkit=$CUDA

echo "Install E2E test dependencies"
pip install openpyxl pymysql gitpython pynvml gspread oauth2client

