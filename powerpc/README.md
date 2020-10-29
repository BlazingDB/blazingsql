# BlazingSQL for PowerPC
## Build and install BlazingSQL
### System dependencies
You need these dependencies, they can be provided by OS package system (e.g. apt/deb, yum/rpm, etc.) or other external package system (e.g. Lmod)
- git
- patch
- bzip2
- wget
- gcc/g++ 7.4.0
- cuda >= 10.1.243
- make
- cmake 3.17.3
- libtool
- openssh-devel
- boost 1.66.0
- zlib-devel
- python (with pip) 3.7.7
- flex
- bison
- byacc
- openblas-devel
- texinfo
- libjpeg-devel
- freetype-devel
- libcurl-devel
- maven
- lsf-tools 2.0
- hwloc
- gdrcopy

If running on Summit and using lmod and your system has the necessary modules you can use:
```shell
module load gcc/7.4.0
module load python/3.7.0
module load cmake/3.17.3
module load boost/1.66.0
module load cuda/10.1.243
module load zlib
module load texinfo
module load openblas
module load netlib-lapack
module load hwloc
module load gdrcopy
```

### Setup the environment
Using regular python you just need to make sure that you have an environment.
NOTE: if using Lmod, make sure you have loaded your python packages before creating or starting your python environment.
First lets define an environment variable to keep track of the environment path:
```shell
export VIRTUAL_ENV=PATH_TO_YOUR_ENV_PREFIX
mkdir $VIRTUAL_ENV
```
If you don't have an environment you can create it with:
```shell
python -m venv $VIRTUAL_ENV
```
Before run any command first you need to activate your environment with:
```shell
source $VIRTUAL_ENV/bin/activate
```


### Build & install BlazingSQL
It is recommended you setup a build folder and export to the following variable before you begin building:
`export BLAZINGSQL_POWERPC_TMP_BUILD_DIR=PATH_TO_A_BUILD_FOLDER`

Run the build script and pass your environment folder (prefix path) as argument:
```shell
cd blazingsql
bash powerpc/build.sh $VIRTUAL_ENV
```

It is recommented to pipe the output of the installation to a file, so that if the terminal is closed or something goes wrong
you can maintain a copy of the installation output:
```shell
cd blazingsql
bash powerpc/build.sh $VIRTUAL_ENV | tee out.txt
```

Notes:
* You need to run the build process from the root directory of the project: *blazingsql*
* Near the end of the process, you will be prompted to answer some questions for the installation of JAVA.
* This build process will install cudf and its dependencies (dask-cudf, arrow, etc.), llvm, compiled python packages like (llvmlite, cupy, etc.) and blazingsql.


### Build & install BlazingSQL on Summit
The `summit_install.sh` script will create the python virtual environment for you an load all the lmod modules for you.

It is recommended you setup a build folder and export to the following variable before you begin building:
`export BLAZINGSQL_POWERPC_TMP_BUILD_DIR=PATH_TO_A_BUILD_FOLDER`

Run the installation script and pass your environment folder (prefix path) as argument:
```shell
cd blazingsql
bash powerpc/summit_install.sh $VIRTUAL_ENV
```

It is recommented to pipe the output of the installation to a file, so that if the terminal is closed or something goes wrong
you can maintain a copy of the installation output:
```shell
cd blazingsql
bash powerpc/summit_install.sh $VIRTUAL_ENV | tee out.txt
```

Notes:
* You need to run the build process from the root directory of the project: *blazingsql*
* Near the end of the process, you will be prompted to answer some questions for the installation of JAVA.
* This build process will install cudf and its dependencies (dask-cudf, arrow, etc.), llvm, compiled python packages like (llvmlite, cupy, etc.) and blazingsql.



## Use BlazingSQL
For now we need to export some env vars before run python with blazingsql:
```shell
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64/:/usr/local/lib
export LD_LIBRARY_PATH=$VIRTUAL_ENV/lib:$LD_LIBRARY_PATH
export CONDA_PREFIX=$VIRTUAL_ENV
export PATH=$BLAZINGSQL_POWERPC_TMP_BUILD_DIR/ibm-java-ppc64le-80/bin:$PATH
export JAVA_HOME=$BLAZINGSQL_POWERPC_TMP_BUILD_DIR/ibm-java-ppc64le-80/jre
```
Note: We don't need conda, we just export CONDA_PREFIX because in some places blazingsql uses that env var as default prefix.
