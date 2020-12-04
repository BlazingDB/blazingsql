from setuptools import find_packages, setup

def get_version():
    import subprocess
    git_out = subprocess.Popen(["git", "describe", "--abbrev=0", "--tags"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout,stderr = git_out.communicate()
    version = "0+unknown"
    if len(stderr.decode())==0:
        version = stdout.split()[0].decode()
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
