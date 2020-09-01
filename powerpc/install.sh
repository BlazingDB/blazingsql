#!/bin/bash
# usage: /new-path/python/environment/

VIRTUAL_ENV=$1

echo "### Modules loading ###"
module load gcc/7.4.0
module load python/3.7.0
module load cmake/3.17.3
module load boost/1.66.0
module load cuda/10.1.243
module load zlib
module load texinfo
module load openblas
module list

echo "### Modules loading ###"
python -m venv $VIRTUAL_ENV
source $VIRTUAL_ENV/bin/activate

echo "### Python dependencies ###"
pip install cython==0.29.21
pip install --no-binary numpy numpy==1.13.3

# pip install -r requirements.txt
pip install numba==0.50.1
pip install scikit-learn==0.23.1
pip install flake8==3.8.3
pip install ipython==7.17.0
pip install pytest-timeout==1.4.2
pip install sphinx-rtd-theme==0.5.0
pip install cysignals==1.10.2
pip install numpydoc==1.1.0
pip install scipy==1.5.2
pip install pynvml==8.0.4
pip install cysignals==1.10.2
pip install networkx==2.4
pip install jupyterlab==2.2.4
pip install notebook==6.1.3
pip install joblib==0.16.0
pip install fastrlock==0.5
pip install pytest-timeout==1.4.2
pip install hypothesis==5.26.0
pip install python-louvain==0.14
pip install jupyter-server-proxy==1.5.0
pip install statsmodels==0.11.1
pip install pyhive==0.6.2
pip install thrift==0.13.0
pip install jpype1==1.0.2
pip install netifaces==0.10.9

echo "### Building ###"
./build.sh $VIRTUAL_ENV
