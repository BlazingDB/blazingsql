from setuptools import find_packages, setup

def get_version():
    import os
    version = os.system("git describe --abbrev=0 --tags")
    print("## VERSION ## %s", version)
    return version

install_requires = ["pyhive", "cudf", "dask-cudf", "dask", "distributed"]

setup(
    name="blazingsql",
    version=get_version(),
    description="BlazingSQL engine",
    url="https://github.com/BlazingDB/pyBlazing/",
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
    packages=find_packages(
        include=["pyblazing", "pyblazing.*", "blazingsql", "blazingsql.*"]
    ),
    install_requires=install_requires,
    zip_safe=False,
)
