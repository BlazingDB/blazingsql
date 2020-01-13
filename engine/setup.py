# Copyright (c) 2019, BlazingSQL.
import os
import sysconfig
from distutils.sysconfig import get_python_lib

import numpy as np

from Cython.Build import cythonize
from setuptools import find_packages, setup
from setuptools.extension import Extension

install_requires = ["cudf", "numba", "cython"]

conda_env_dir = os.environ["CONDA_PREFIX"]

if os.environ.get('CONDA_BUILD') is not None:
    if os.environ["CONDA_BUILD"] == "1":
        conda_env_dir = os.environ["BUILD_PREFIX"]

conda_env_inc = os.path.join(conda_env_dir, "include")

conda_env_inc_cudf = os.path.join(conda_env_inc, "cudf")
conda_env_inc_cub = os.path.join(conda_env_inc, "bsql-rapids-thirdparty/cub")
conda_env_inc_libcudacxx = os.path.join(conda_env_inc, "bsql-rapids-thirdparty/libcudacxx/include")

# TODO percy c.gonzales fix blazingdb-io headers
conda_env_inc_io = os.path.join(conda_env_inc, "blazingdb/io")
conda_env_inc_communication = os.path.join(conda_env_inc, "blazingdb/communication")

conda_env_lib = os.path.join(conda_env_dir, "lib")

print("Using CONDA_PREFIX : " + conda_env_dir)

cython_files = ["bsql_engine/io/io.pyx"]

extensions = [
    Extension(
        "cio",
        sources=cython_files,
        include_dirs=[
            "include/",
            "src/",
            conda_env_inc,
            conda_env_inc_cudf,
            conda_env_inc_cub,
            conda_env_inc_libcudacxx,
            conda_env_inc_io,
            conda_env_inc_communication,
            "/usr/local/cuda/include",
            os.path.dirname(
                sysconfig.get_path("include")),
            np.get_include(),
        ],
        library_dirs=[
            get_python_lib(),
            conda_env_lib,
            os.path.join(
                os.sys.prefix,
                "lib")],
        libraries=["blazingsql-engine"],
        language="c++",
        extra_compile_args=["-std=c++14"],
    )]

setup(
    name="bsql_engine",
    version="0.6",
    description="BlazingSQL engine bindings",
    url="https://github.com/blazingdb/blazingdb-ral",
    author="BlazingSQL",
    license="Apache 2.0",
    classifiers=[
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    # Include the separately-compiled shared library
    setup_requires=["cython"],
    ext_modules=cythonize(extensions),
    packages=find_packages(include=["bsql_engine", "bsql_engine.*"]),
    package_data={
        "bsql_engine.io": ["bsql_engine/io/cio.pxd"],
        "bsql_engine.io": ["*.pxd"],
        "bsql_engine.io": ["cio.pxd"],
        "bsql_engine": ["bsql_engine/io/cio.pxd"]
    },
    install_requires=install_requires,
    zip_safe=False,
)
