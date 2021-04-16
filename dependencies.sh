#!/bin/bash
#
# This script creates a conda environment with dependencies
#

read -e -p 'Enter rapids version (Ejem. 0.18): ' RAPIDS_VERSION
read -e -p 'Enter cuda version (Ejem. 10.2): ' CUDA_VERSION
read -e -p 'Enter python version: (Ejem. 3.7)' PYTHON_VERSION
read -e -p 'Enter is nightly (yes/no): ' NIGHTLY

if [ -z $RAPIDS_VERSION ]; then
  RAPIDS_VERSION=0.20
fi

if [ -z $CUDA_VERSION ]; then
  CUDA_VERSION=11.0
fi

if [ -z $PYTHON_VERSION ]; then
  PYTHON_VERSION=3.7
fi

if [ -z $NIGHTLY ]; then
  NIGHTLY="yes"
fi


if [ $RAPIDS_VERSION >= 0.20 ]; then
  echo "Installing rapids $RAPIDS_VERSION dependencies"
  conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.25 ninja nlohmann_json mysql-connector-cpp=8.0.23 libpq=13
  conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS_VERSION dask-cudf=$RAPIDS_VERSION cudf=$RAPIDS_VERSION ucx-py=$RAPIDS_VERSION ucx-proc=*=gpu python=$PYTHON_VERSION cudatoolkit=$CUDA_VERSION
  conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive tqdm ipywidgets
fi

if [ $RAPIDS_VERSION == 0.19 ]; then
  echo "Installing rapids $RAPIDS_VERSION dependencies"
  conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.25 ninja nlohmann_json numpy=1.20
  conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS_VERSION dask-cudf=$RAPIDS_VERSION cudf=$RAPIDS_VERSION ucx-py=$RAPIDS_VERSION ucx-proc=\*=gpu python=$PYTHON_VERSION cudatoolkit=$CUDA_VERSION
  conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive tqdm ipywidgets
fi

if [ $RAPIDS_VERSION == 0.18 ]; then
  echo "Installing rapids $RAPIDS_VERSION dependencies"
  conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.25 ninja nlohmann_json mysql-connector-cpp=8.0.23 libpq=13
  conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults dask-cuda=$RAPIDS_VERSION dask-cudf=$RAPIDS_VERSION cudf=$RAPIDS_VERSION ucx-py=$RAPIDS_VERSION ucx-proc=*=gpu python=$PYTHON_VERSION cudatoolkit=$CUDA_VERSION
  conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive tqdm ipywidgets
fi
