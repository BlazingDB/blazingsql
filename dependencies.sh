#!/bin/bash
#
# This script install BlazingSQL dependencies based on rapids version
#
echo "Usage: ./$0 rapids_version cuda_version nightly"

export GREEN='\033[0;32m'
export RED='\033[0;31m'
BOLDGREEN="\e[1;${GREEN}"
ITALICRED="\e[3;${RED}"
ENDCOLOR="\e[0m"

RAPIDS_VERSION="0.20"
CUDA_VERSION="11.0"
CHANNEL=""

if [ ! -z $1 ]; then
  RAPIDS_VERSION=$1
fi

if [ ! -z $2 ]; then
  CUDA_VERSION=$2
fi

if [ ! -z $3 ]; then
  CHANNEL="-nightly"
fi

echo -e "${GREEN}Installing dependencies${ENDCOLOR}"
conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.25 ninja mysql-connector-cpp=8.0.23 libpq=13 nlohmann_json=3.9.1
# NOTE cython must be the same of cudf (for 0.11 and 0.12 cython is >=0.29,<0.30)
conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive pytest tqdm ipywidgets


echo -e "${GREEN}Install RAPIDS dependencies${ENDCOLOR}"
conda install --yes -c rapidsai$CHANNEL -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS_VERSION dask-cudf=$RAPIDS_VERSION cudf=$RAPIDS_VERSION ucx-py=$RAPIDS_VERSION ucx-proc=*=gpu cudatoolkit=$CUDA_VERSION

echo-e "${GREEN}Install E2E test dependencies${ENDCOLOR}"
pip install openpyxl pymysql gitpython pynvml gspread oauth2client

if [ $? -eq 0 ]; then
  echo -e "${GREEN}Installation complete${ENDCOLOR}"
else
  echo -e "${RED}Installation failed${ENDCOLOR}"
fi
