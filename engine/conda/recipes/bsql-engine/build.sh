#!/bin/bash

if [ -z ${1+x} ]
then
   INSTALL_PREFIX=$CONDA_PREFIX
else
   INSTALL_PREFIX=$1
fi

rm -f blazing-cython/io/io.cpp

export CXXFLAGS="-L$INSTALL_PREFIX/lib"
export CFLAGS=$CXXFLAGS
export LDFLAGS=$CXXFLAGS

python setup.py build_ext --inplace
if [ $? != 0 ]; then
  exit 1
fi
python setup.py install --single-version-externally-managed --record=record.txt
if [ $? != 0 ]; then
  exit 1
fi

if [[ $CONDA_BUILD -eq 1 ]]
then
   cp `pwd`/cio*.so `pwd`/../../_h_env*/lib/python*/site-packages
   cp -r `pwd`/bsql_engine `pwd`/../../_h_env*/lib/python*/site-packages
fi
