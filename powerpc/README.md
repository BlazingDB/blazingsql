# BlazingSQL for PowerPC
## User documentation
### Build and install BlazingSQL
```shell
cd blazingsql/powerpc
./build.sh PATH_TO_YOUR_ENV_PREFIX
```

### Use BlazingSQL
For now we need to export some env vars before run a python with blazingsql:
```shell
export JAVA_HOME=/usr/lib/jvm/jre
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64/:/usr/local/lib # optional
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:PATH_TO_YOUR_ENV_PREFIX
export CONDA_PREFIX=PATH_TO_YOUR_ENV_PREFIX
```

Note: We don't need conda, we just export CONDA_PREFIX becouse in some places the blazingsql build system uses that env var as default prefix.
