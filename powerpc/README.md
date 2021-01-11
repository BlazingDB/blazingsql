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
- nccl

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
source powerpc/build.sh $VIRTUAL_ENV
```

It is recommented to pipe the output of the installation to a file, so that if the terminal is closed or something goes wrong
you can maintain a copy of the installation output:
```shell
cd blazingsql
source powerpc/build.sh $VIRTUAL_ENV | tee out.txt
```

Notes:
* You need to run the build process from the root directory of the project: *blazingsql*
* Near the end of the process, you will be prompted to answer some questions for the installation of JAVA.
* This build process will install cudf and its dependencies (dask-cudf, arrow, etc.), llvm, compiled python packages like (llvmlite, cupy, etc.) and blazingsql.


### Build & install BlazingSQL on Summit
The following instructions are for building on Summit. They are similar to the instructions above.

**The order of the following steps is important, because the python module needs to be loaded first before creating the virtual environment.**

1. Use lmod to load up all the dependencies.
```shell
unset CMAKE_PREFIX_PATH # make sure no conflicting library versions are found
module load gcc/7.4.0
module load python/3.7.0
module load cmake/3.18.2
module load boost/1.66.0
module load cuda/10.1.243
module load zlib
module load texinfo
module load openblas
module load netlib-lapack
# for UCX BEGIN
module load hwloc
module load gdrcopy
# for UCX END
module list
```

2. Export the environment variables for your Virtual Environment and Build folder and make sure the folders exist:
```shell
export VIRTUAL_ENV=PATH_TO_YOUR_ENV_PREFIX
mkdir $VIRTUAL_ENV
export BLAZINGSQL_POWERPC_TMP_BUILD_DIR=PATH_TO_A_BUILD_FOLDER
mkdir $BLAZINGSQL_POWERPC_TMP_BUILD_DIR
```

3. Create your virtual environment and activate it:
```shell
python -m venv $VIRTUAL_ENV
source $VIRTUAL_ENV/bin/activate
```

4. Download and install nccl
* download local installer from https://developer.nvidia.com/nccl
* (have to register and answer questionnaire)
* Power  "O/S agnostic local installer"
```shell
tar xvfk nccl_2.7.6-1+cuda10.1_ppc64le.txz
cp -r nccl_2.7.6-1+cuda10.1_ppc64le/lib/* $VIRTUAL_ENV/lib/
cp -r nccl_2.7.6-1+cuda10.1_ppc64le/include/* $VIRTUAL_ENV/include/
```

5. Make sure you are in the `blazingsql` folder and run the build script and pass your environment folder as argument:
```shell
cd blazingsql
nohup sh powerpc/build.sh $VIRTUAL_ENV &
```

Notes:
* You need to run the build process from the root directory of the project: *blazingsql*
* Near the end of the process, you will be prompted to answer some questions for the installation of JAVA.
* This build process will install cudf and its dependencies (dask-cudf, arrow, etc.), llvm, compiled python packages like (llvmlite, cupy, etc.) and blazingsql.

6. Add the library directory to your `LD_LIBRARY_PATH`, e.g. upon activation of the environment:
```shell
patch $VIRTUAL_ENV/bin/activate powerpc/activate.patch
```
## Use BlazingSQL
For now we need to export some env vars before run python with blazingsql:
```shell
export CONDA_PREFIX=$VIRTUAL_ENV
export PATH=$BLAZINGSQL_POWERPC_TMP_BUILD_DIR/ibm-java-ppc64le-80/bin:$PATH
export JAVA_HOME=$BLAZINGSQL_POWERPC_TMP_BUILD_DIR/ibm-java-ppc64le-80/jre
```
Note: We don't need conda, we just export CONDA_PREFIX because in some places blazingsql uses that env var as default prefix.
