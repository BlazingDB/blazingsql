#!/bin/bash

conda install --yes -c conda-forge spdlog=1.7.0 google-cloud-cpp=1.25 ninja nlohmann_json numpy=1.20
conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults dask-cuda=0.19 dask-cudf=0.19 cudf=0.19 ucx-py=0.19 ucx-proc=*=gpu python=3.7 cudatoolkit=10.2
conda install --yes -c conda-forge cmake=3.18 gtest==1.10.0=h0efe328_4 gmock cppzmq cython=0.29 openjdk=8.0 maven jpype1 netifaces pyhive tqdm ipywidgets
