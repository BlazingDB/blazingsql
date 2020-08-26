# BlazingSQL for PowerPC
## Build and install BlazingSQL
### System dependencies
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
### Setup the environment
Using regular python you just need to make sure that you have an environment.
If you don't have an environment you can create it with:
```shell
python -m venv PATH_TO_YOUR_ENV_PREFIX
```
Before run any command first you need to activate your environment with:
```shell
source PATH_TO_YOUR_ENV_PREFIX/bin/activate
```
Then install the python dependencies in your environment:
```shell
pip install -r blazingsql/powerpc/requirements.txt
```
### Build & install BlazingSQL
Run the build script and pass your environment folder (prefix path) as argument:
```shell
cd blazingsql/powerpc
./build.sh PATH_TO_YOUR_ENV_PREFIX
```
After the process finish you will have BlazingSQL installed and ready in your environment.
## Use BlazingSQL
For now we need to export some env vars before run python with blazingsql:
```shell
export JAVA_HOME=/usr/lib/jvm/jre
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64/:/usr/local/lib # optional
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:PATH_TO_YOUR_ENV_PREFIX
export CONDA_PREFIX=PATH_TO_YOUR_ENV_PREFIX
```
Note: We don't need conda, we just export CONDA_PREFIX because in some places blazingsql uses that env var as default prefix.
