#!/bin/bash

echo "### start build.sh ### "

python setup.py build_ext --inplace
python setup.py install --single-version-externally-managed --record=record.txt

if [[ $CONDA_BUILD -eq 1 ]]
then
   cp -r `pwd`/pyblazing `pwd`/../../_h_env*/lib/python*/site-packages
   cp -r `pwd`/blazingsql `pwd`/../../_h_env*/lib/python*/site-packages
fi
