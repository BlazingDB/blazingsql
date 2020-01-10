#!/bin/bash

echo "### start build.sh ### "

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
   cp -r `pwd`/pyblazing `pwd`/../../_h_env*/lib/python*/site-packages
   cp -r `pwd`/blazingsql `pwd`/../../_h_env*/lib/python*/site-packages
fi
