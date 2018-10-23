import os
import shutil
import subprocess
import sys
import tempfile

from setuptools import setup, find_packages

setup(
  name='PyBlazing',
  version='0.1',
  author='BlazingDB Team',
  packages=find_packages(),
  install_requires=(
    'numpy==1.15.2',
    'pycuda==2018.1.1',
    'pygdf@git+https://github.com/rapidsai/pygdf.git#egg=pygdf-master',
  ),
  zip_safe=False,
)
