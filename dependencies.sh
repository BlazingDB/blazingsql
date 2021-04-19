#!/bin/bash
#
# This script creates a conda environment with dependencies
#
export GREEN='\033[0;32m'
export RED='\033[0;31m'
BOLDGREEN="\e[1;${GREEN}"
ITALICRED="\e[3;${RED}"
ENDCOLOR="\e[0m"

read -e -p 'Enter rapids version (default 0.20): ' RAPIDS_VERSION
read -e -p 'Enter cuda version (default 11.0): ' CUDA_VERSION
read -e -p 'Enter python version: (default 3.7): ' PYTHON_VERSION
read -e -p 'Create a conda environment (yes/no default yes): ' CONDA

if [ -z $RAPIDS_VERSION ]; then
  RAPIDS_VERSION=0.20
fi

if [ -z $CUDA_VERSION ]; then
  CUDA_VERSION=11.0
fi

if [ -z $PYTHON_VERSION ]; then
  PYTHON_VERSION=3.7
fi

if [ -z $CONDA ]; then
  CONDA="yes"
fi

if [ $CONDA == "yes" ]; then
  ENV_NAME="bsql-rapids$RAPIDS_VERSION-cuda$CUDA_VERSION-py$PYTHON_VERSION"
  echo -e "${BOLDGREEN}Creating conda environment $ENV_NAME ${ENDCOLOR}"
  conda create --yes python=$PYTHON_VERSION -n $ENV_NAME
  source ~/.profile
  eval "$(conda shell.bash hook)"
  conda activate $ENV_NAME
fi
message="${GREEN}Installing rapids $RAPIDS_VERSION, cuda $CUDA_VERSION and python $PYTHON_VERSION dependencies${ENDCOLOR}"
if [ "$RAPIDS_VERSION" == "0.20" ]; then
  echo -e $message
  conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.25 ninja nlohmann_json mysql-connector-cpp=8.0.23 libpq=13
  conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS_VERSION dask-cudf=$RAPIDS_VERSION cudf=$RAPIDS_VERSION ucx-py=$RAPIDS_VERSION ucx-proc=*=gpu python=$PYTHON_VERSION cudatoolkit=$CUDA_VERSION
  conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive tqdm ipywidgets
fi

if [ "$RAPIDS_VERSION" == "0.19" ]; then
  echo -e $message
  conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.25 ninja nlohmann_json numpy=1.20
  conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS_VERSION dask-cudf=$RAPIDS_VERSION cudf=$RAPIDS_VERSION ucx-py=$RAPIDS_VERSION ucx-proc=\*=gpu python=$PYTHON_VERSION cudatoolkit=$CUDA_VERSION
  conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive tqdm ipywidgets
fi

if [ "$RAPIDS_VERSION" == "0.18" ]; then
  echo -e $message
  conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.16 ninja
  conda install --yes -c rapidsai -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS_VERSION dask-cudf=$RAPIDS_VERSION cudf=$RAPIDS_VERSION ucx-py=$RAPIDS_VERSION ucx-proc=*=gpu python=$PYTHON_VERSION cudatoolkit=$CUDA_VERSION
  conda install --yes -c conda-forge cmake=3.18 gtest gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive tqdm ipywidgets
fi

if [ $? -eq 0 ]; then
  echo -e "${GREEN}Installation complete${ENDCOLOR}"
  echo -e "${GREEN}Execute this command to activate your conda environment: conda activate $ENV_NAME${ENDCOLOR}"
else
  echo -e "${RED}Installation failed${ENDCOLOR}"
fi
