# Copyright (c) 2019, BlazingSQL.
import os
import sysconfig
from distutils.sysconfig import get_python_lib

from setuptools.command.build_ext import build_ext

import numpy as np
from Cython.Build import cythonize
from setuptools import find_packages, setup
from setuptools.extension import Extension

install_requires = ["cudf", "numba", "cython"]

conda_env_dir = os.environ["CONDA_PREFIX"]

if os.environ.get("CONDA_BUILD") is not None:
    if os.environ["CONDA_BUILD"] == "1":
        conda_env_dir = os.environ["BUILD_PREFIX"]

conda_env_inc = os.path.join(conda_env_dir, "include")

conda_env_inc_cudf = os.path.join(conda_env_inc, "cudf")
conda_env_inc_cub = os.path.join(conda_env_inc, "bsql-rapids-thirdparty/cub")
conda_env_inc_libcudacxx = os.path.join(
    conda_env_inc, "libcudf/libcudacxx"
)

# TODO percy c.gonzales fix blazingdb-io headers
conda_env_inc_io = os.path.join(conda_env_inc, "blazingdb/io")
conda_env_inc_manager = os.path.join(conda_env_inc, "blazingdb/manager")
conda_env_inc_communication = os.path.join(conda_env_inc,
                                           "blazingdb/communication")
conda_env_lib = os.path.join(conda_env_dir, "lib")

print("Using CONDA_PREFIX : " + conda_env_dir)

def use_gtest_lib():
    bt_sock = os.popen('grep BUILD_TYPE build/CMakeCache.txt')
    bt = bt_sock.read()
    bt_val = bt.split("=")[1].strip()

    if bt_val == "Release" or bt_val == "RelWithDebInfo":
        print("Cython RAL wrapper: This is a release build so the final runtime binaries will not depend on gtest")
        return False

    print("Cython RAL wrapper: This is not a release build so the final runtime binaries will depend on gtest (BSQLDBGUTILS precompiler definition is set)")
    return True

def get_libs():
    ret = ["blazingsql-engine"]
    if use_gtest_lib():
        ret.append("gtest")
    return ret

class BuildExt(build_ext):
    def build_extensions(self):
        if '-Wstrict-prototypes' in self.compiler.compiler_so:
            self.compiler.compiler_so.remove('-Wstrict-prototypes')
        super().build_extensions()


cython_files = ["bsql_engine/io/io.pyx"]

extensions = [
    Extension(
        "cio",
        sources=cython_files,
        include_dirs=[
            "include/",
            "src/",
            os.path.dirname(sysconfig.get_path("include")),
        ],
        library_dirs=[
            get_python_lib(),
            conda_env_lib,
            os.path.join(os.sys.prefix, "lib"),
        ],
        libraries=get_libs(),
        language="c++",
        extra_compile_args=["-std=c++17",
                            "-Wno-unknown-pragmas",
                            "-Wno-unused-variable",
                            "-Wno-unused-function",
                            '-isystem' + conda_env_inc,
                            '-isystem' + conda_env_inc_cudf,
                            '-isystem' + conda_env_inc_cub,
                            '-isystem' + conda_env_inc_libcudacxx,
                            '-isystem' + conda_env_inc_io,
                            '-isystem' + conda_env_inc_communication,
                            '-isystem' + conda_env_inc_manager,
                            '-isystem' + "/usr/local/cuda/include",
                            '-isystem' + np.get_include()],
    )
]

setup(
    name="bsql_engine",
    version="0.6",
    description="BlazingSQL engine bindings",
    url="https://github.com/blazingdb/blazingdb-ral",
    author="BlazingSQL",
    cmdclass={'build_ext': BuildExt},
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
    # TODO: force comment to pass style issue
    package_data={
        "bsql_engine.io": ["cio.pxd"],
        "bsql_engine": ["bsql_engine/io/cio.pxd"],
    },
    install_requires=install_requires,
    zip_safe=False,
)

