#!/bin/bash

#module load gcc/7.4

cd /tmp
wget -q https://github.com/Kitware/CMake/releases/download/v3.18.2/cmake-3.18.2.tar.gz
tar -xvf cmake-3.18.2.tar.gz
cd cmake-3.18.2
./bootstrap --prefix=/opt/sw/cmake-3.18.2/
make -j`nproc`
make install
