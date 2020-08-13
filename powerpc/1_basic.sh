#!/bin/bash
# TODO installed based on libraries
# 

set -e

# Before run you can export:
# export PARALLEL_LEVEL=4

WORLDWORK=$PWD

#echo export CC=\$OLCF_GCC_ROOT/bin/gcc >> ~/.bashrc
#echo export CXX=\$OLCF_GCC_ROOT/bin/g++ >> ~/.bashrc
#source ~/.bashrc

export CC=/usr/local/bin/gcc
export CXX=/usr/local/bin/g++
export CUDACXX=/usr/local/cuda/bin/nvcc

VIRTUAL_ENV=$PWD/rapids-env$1
BUILD_SRC=$VIRTUAL_ENV/build
mkdir -p $BUILD_SRC

# Virtual Environment
python -m venv $VIRTUAL_ENV
source $VIRTUAL_ENV/bin/activate

# Cargando versiones
source ./versions.properties
#cd $VIRTUAL_ENV/

echo "##### Env #####"
echo "VIRTUAL_ENV: "$VIRTUAL_ENV
echo "BUILD_SRC: "$BUILD_SRC
echo "gcc: "`gcc --version`
echo "g++: "`g++ --version`
#echo "nvcc: "`nvcc --version`
echo "PATH: "$PATH
echo "LD_LIBRARY_PATH: "$LD_LIBRARY_PATH



#cd $BUILD_SRC/
#echo "## Abseil"
#if [ ! -d "abseil-cpp" ]; then
#  git clone -b $ABSEIL_V https://github.com/abseil/abseil-cpp
#  mkdir -p abseil-cpp/build/
#  cd abseil-cpp/build/
#  cmake -DBUILD_SHARED_LIBS=ON -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../
#  make -j 8 install
#else
#  echo "Skipping"
#fi


cd $BUILD_SRC/
echo "## GTest"
if [ ! -d "googletest" ]; then
  git clone -b $GTEST_V https://github.com/google/googletest.git
  mkdir -p googletest/build/
  cd googletest/build/
  cmake -DBUILD_SHARED_LIBS=ON -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../
  make -j$PARALLEL_LEVEL install
else
  echo "Skipping"
fi


cd $BUILD_SRC/
echo "## Protobuf"
if [ ! -d "protobuf" ]; then
  git clone -b $PROTOBUF_V https://github.com/protocolbuffers/protobuf.git
  mkdir -p protobuf/build/
  cd protobuf/
  git submodule update --init --recursive
  cd build/
  cmake -D protobuf_BUILD_TESTS=OFF -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -D CMAKE_BUILD_TYPE=Release ../cmake
  make -j$PARALLEL_LEVEL install
else
  echo "Skipping"
fi


cd $BUILD_SRC/
echo "## C-Ares"
if [ ! -d "c-ares" ]; then
  git clone -b $CARES_V https://github.com/c-ares/c-ares
  cd c-ares/
  ./buildconf
  ./configure --prefix=$VIRTUAL_ENV
  make -j$PARALLEL_LEVEL install
else
  echo "Skipping"
fi


cd $BUILD_SRC/
echo "## Re2"
if [ ! -d "re2" ]; then
  git clone -b $RE2_V https://github.com/google/re2.git
  mkdir -p re2/build/
  cd re2/build/
  cmake -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -DCMAKE_BUILD_TYPE=RELEASE ../
  make -j$PARALLEL_LEVEL install
else
  echo "Skipping"
fi


cd $BUILD_SRC/
echo "## grpc++"
if [ ! -d "grpc" ]; then
  # TODO Mario siempre usar --depth 1 para los clones en este script
  git clone --depth 1 -b $GRPC_V https://github.com/grpc/grpc
  #git clone https://github.com/grpc/grpc
  cd grpc/
  #git checkout v1.31.0
  export GRPC_SRC=$PWD
  echo "GRPC_SRC: "$GRPC_SRC
  git submodule update --init --recursive
  cd third_party/abseil-cpp
  # see https://github.com/abseil/abseil-cpp/pull/739
  git remote add ppc_fix https://github.com/jglaser/abseil-cpp.git
  git fetch ppc_fix
  git checkout ppc_fix/fix_ppc_build
  mkdir -p $GRPC_SRC/build
  cd $GRPC_SRC/build
  echo "CMAKE"
  #cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -DCMAKE_BUILD_TYPE=Release $GRPC_SRC
  
  echo "MAKE"
  #make -j$PARALLEL_LEVEL grpc_cpp_plugin install
  #make -j$PARALLEL_LEVEL grpc_cpp_plugin

cmake .. -DBUILD_SHARED_LIBS=ON \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH=$VIRTUAL_ENV \
      -DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV \
      -DgRPC_CARES_PROVIDER="package" \
      -DgRPC_GFLAGS_PROVIDER="package" \
      -DgRPC_PROTOBUF_PROVIDER="package" \
      -DProtobuf_ROOT=$VIRTUAL_ENV \
      -DgRPC_SSL_PROVIDER="package" \
      -DgRPC_ZLIB_PROVIDER="package" \
      -DgRPC_ABSL_PROVIDER="package"

cmake --build . --config Release --target grpc_cpp_plugin


else
  echo "Skipping"
fi

cd $BUILD_SRC/
echo "## Crc32"
if [ ! -d "crc32" ]; then
  git clone -b $CRC32_V https://github.com/google/crc32c.git
  cd crc32c/
  git submodule update --init --recursive
  mkdir -p build/
  cd build
  cmake -DCRC32_BUILD_BENCHMARKS=OFF -DCRC32C_BUILD_TESTS=OFF -DCRC32C_USE_GLOG=OFF -DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -D CMAKE_BUILD_TYPE=Release ../
  make -j$PARALLEL_LEVEL install
else
  echo "Skipping"
fi

echo "### EXIT ##"
deactivate
exit


cd $BUILD_SRC/
# TODO Issue con patch 
echo "## google-cloud-cpp"
if [ ! -d "google-cloud-cpp" ]; then
  git clone -b $GCLOUD_V https://github.com/googleapis/google-cloud-cpp
  cd google-cloud-cpp
  echo "patch:"
  echo "diff --git a/google/cloud/storage/CMakeLists.txt b/google/cloud/storage/CMakeLists.txt
index 690c292..435f308 100644
--- a/google/cloud/storage/CMakeLists.txt
+++ b/google/cloud/storage/CMakeLists.txt
@@ -232,6 +232,7 @@ target_link_libraries(
            Threads::Threads
            OpenSSL::SSL
            OpenSSL::Crypto
+           crypto
            ZLIB::ZLIB)
 google_cloud_cpp_add_common_options(storage_client)" | patch
  mkdir -p build
  cd build
  cmake -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=OFF -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../
  make -j$PARALLEL_LEVEL install
else
  echo "Skipping"
fi

# TODO issue with curl
echo "## aws sdk cpp"
cd $BUILD_SRC/
git clone -b $AWS_V https://github.com/aws/aws-sdk-cpp.git
mkdir aws-sdk-cpp/build
cd aws-sdk-cpp/build
cmake -DENABLE_TESTING=OFF -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../
make -j$PARALLEL_LEVEL install


echo "## cython"
cd $BUILD_SRC
git clone -b $CYTHON_V https://github.com/cython/cython.git
cd cython
python setup.py install


echo "## rapidjson"
cd $BUILD_SRC
git clone -b $RAPIDJSON_V https://github.com/Tencent/rapidjson.git
cd rapidjson
git submodule update --init
mkdir build
cd build
cmake -D CMAKE_BUILD_TYPE=Release -D RAPIDJSON_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../
make -j$PARALLEL_LEVEL install


echo "## utf8proc"
cd $BUILD_SRC
git clone -b $UTFPROC_V https://github.com/JuliaStrings/utf8proc.git
mkdir utf8proc/build
cd utf8proc/build
cmake -D CMAKE_BUILD_TYPE=RELEASE -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../
make -j$PARALLEL_LEVEL install


# TODO issue with compilation
echo "## Orc"
cd $BUILD_SRC
git clone -b $ORC_V https://github.com/apache/orc
mkdir orc/build
cd orc/build
cmake -D BUILD_SHARED_LIBS=ON -D CMAKE_BUILD_TYPE=Release -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -D BUILD_CPP_TESTS=OFF -D BUILD_JAVA=OFF ../
make -j16 install


## if it errors in snappy test, modify snappy_ep-prefix/src/snappy_ep-build/CMakeCache.txt -> SNAPPY_BUILD_TESTS:BOOL=OFF
echo "## Snappy"
cd $BUILD_SRC
git clone -b $SNAPPY_V https://github.com/google/snappy.git # Apache ORC compatibility
mkdir snappy/build
cd snappy/build
cmake -DBUILD_SHARED_LIBS=ON -DSNAPPY_BUILD_TESTS=OFF -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../
make -j$PARALLEL_LEVEL install


# TODO boost module is required
# thrift C++ library
module load boost/1.66.0
cd $RAPIDS_SRC
git clone -b $THRIFT_V https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh
export PY_PREFIX=$VIRTUAL_ENV
./configure --prefix=$VIRTUAL_ENV --enable-libs --with-cpp=yes --with-haskell=no
make -j$PARALLEL_LEVEL install

# lz4
echo "## Lz4"
cd $BUILD_SRC
git clone -b $LZ4_V https://github.com/lz4/lz4.git
cd lz4
make
PREFIX=$VIRTUAL_ENV make install

# facebook zstd
echo "## Facebook zstd"
cd $BUILD_SRC
git clone -b $ZSTD_V https://github.com/facebook/zstd.git
mkdir zstd/build-tmp
cd zstd/build-tmp
cmake -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../build/cmake
make -j$PARALLEL_LEVEL install


echo "## Numpy"
pip install --no-binary numpy numpy


# TODO issue with boost 1.66
echo "## arrow"
git clone -b $ARROW_V https://github.com/jglaser/arrow # patched release 0.17.1
mkdir arrow/build
cd arrow/cpp
export ARROW_SRC=$PWD
mkdir -p /tmp/$USER/arrow_build

cd /tmp/$USER/arrow_build
module load boost/1.66.0
cmake -DARROW_PARQUET=ON -DARROW_ORC=ON -D ARROW_PYTHON=ON -DARROW_DATASET=ON -DARROW_CUDA=ON -D ARROW_IPC=ON -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -DLZ4_ROOT=$VIRTUAL_ENV -D ARROW_BUILD_SHARED=ON $ARROW_SRC
make -j$PARALLEL_LEVEL install
cd $RAPIDS_SRC/arrow/python
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_ORC=1
export PYARROW_WITH_CUDA=1
python setup.py install
cd ..
python -c "import pyarrow" # test


echo "## spdlog"
cd $RAPIDS_SRC
git clone -b $SPDLOG_V https://github.com/gabime/spdlog.git
mkdir spdlog/build
cd spdlog/build
cmake -DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ../
make -j$PARALLEL_LEVEL install


# echo "## spdlog"
# cd $RAPIDS_SRC
# git clone https://github.com/gabime/spdlog.git
# cd spdlog
# export SPDLOG_SRC=$PWD
# mkdir -p /tmp/$USER/spdlog-build
# cd /tmp/$USER/spdlog-build
# cmake -DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV $SPDLOG_SRC
# make -j 8 install

# #
# # Rapids Memory Management (rmm)
# #
# cd $RAPIDS_SRC
# git clone https://github.com/rapidsai/rmm.git
# cd rmm
# export RMM_SRC=$PWD
# mkdir -p /tmp/$USER/rmm-build
# cd /tmp/$USER/rmm-build
# cmake -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV $RMM_SRC
# make -j 8 install
# cd $RAPIDS_SRC/rmm/python
# python setup.py install

# #
# # Rapids DLPack (Deep Learning)
# #
# module load boost/1.66.0
# cd $RAPIDS_SRC
# git clone https://github.com/rapidsai/dlpack.git
# cd dlpack
# export DLPACK_SRC=$PWD
# mkdir -p /tmp/$USER/dlpack-build
# cd /tmp/$USER/dlpack-build
# cmake -DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV $DLPACK_SRC
# make -j16 install

# #
# # cudf
# # 

# # pandas
# cd $RAPIDS_SRC
# git clone https://github.com/pandas-dev/pandas.git
# cd pandas
# python setup.py install

# # cupy python dependency
# cd $RAPIDS_SRC
# git clone --recurse-submodules https://github.com/cupy/cupy.git
# cd cupy
# python setup.py install # ignore the cudnn/cutensor/nccl errors, they are non-fatal

# # gold & llvm are for getting llvmlite up and running

# # install gold linker for LLVM -lto
# module load texinfo
# cd $RAPIDS_SRC
# git clone --depth 1 git://sourceware.org/git/binutils-gdb.git binutils
# cd binutils
# ./configure --prefix=$VIRTUAL_ENV --enable-gold --enable-plugins --disable-werror
# make all-gold -j16
# cp gold/ld-new $VIRTUAL_ENV/bin/ld
# make -j16
# cp binutils/ar $VIRTUAL_ENV/bin/ar
# cp binutils/nm-new $VIRTUAL_ENV/bin/nm

# # compile patched LLVM 9 (static libs) using gold linker (LLVMgold.so req'd by llvmlite 0.33)
# # https://llvm.org/docs/GoldPlugin.html (see there for a simple test case)
# # not sure if all of this patching and linker setup is REALLY necessary
# cd $RAPIDS_SRC
# wget https://raw.githubusercontent.com/numba/llvmlite/master/conda-recipes/0001-Revert-Limit-size-of-non-GlobalValue-name.patch
# git clone https://github.com/llvm/llvm-project
# cd llvm-project
# git checkout release/9.x
# export LLVM_SRC=$PWD
# cd llvm
# patch -p1 < ../../0001-Revert-Limit-size-of-non-GlobalValue-name.patch
# mkdir -p $MEMBERWORK/bif128/llvm-build
# cd $MEMBERWORK/bif128/llvm-build
# cmake -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -DLLVM_ENABLE_PROJECTS=clang -DCMAKE_BUILD_TYPE=Release  -DLLVM_TARGETS_TO_BUILD="PowerPC" -D LLVM_BINUTILS_INCDIR=$RAPIDS_SRC/binutils/include $LLVM_SRC/llvm
# make -j 16 install

# # bsub -W 2:00 -P BIF128 -nnodes 1 -Is $SHELL
# # jsrun -n 1 -c 42 -b none make -j 140 install


# # install LLVMgold.so as plugin to 'ar'
# mkdir -p $VIRTUAL_ENV/lib/bfd-plugins
# cp $VIRTUAL_ENV/lib/LLVMgold.so $VIRTUAL_ENV/lib/bfd-plugins

# # llvmlite python-depency (via numba)
# pip uninstall llvmlite
# cd $RAPIDS_SRC
# git clone https://github.com/numba/llvmlite.git
# cd llvmlite
# # https://lists.llvm.org/pipermail/llvm-dev/2019-November/137322.html
# python setup.py install
# cd ..
# python -c "import numba" # test

# # numba
# pip uninstall numba
# cd $RAPIDS_SRC
# git clone https://github.com/numba/numba.git
# cd numba
# python setup.py install

# # restore default linker
# cd $VIRTUAL_ENV/bin
# mv ar ar-new
# mv nm nm-new
# mv ld ld.gold

# # python snappy

# # fsspec
# cd $RAPIDS_SRC
# pip install --no-binary fsspec fsspec

# # snappy bindings
# cd $RAPIDS_SRC
# git clone https://github.com/andrix/python-snappy.git
# cd python-snappy
# CFLAGS=-L$VIRTUAL_ENV/lib python setup.py install

# cd $RAPIDS_SRC
# CUDF_HOME=$(pwd)/cudf
# git clone https://github.com/rapidsai/cudf.git $CUDF_HOME
# cd $CUDF_HOME
# git submodule update --init --remote --recursive
# mkdir -p $MEMBERWORK/bif128/cudf-build
# cd $MEMBERWORK/bif128/cudf-build
# module load boost/1.66.0
# cmake -D GPU_ARCHS=70 -DBUILD_TESTS=ON -DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -DCMAKE_CXX11_ABI=ON $CUDF_HOME/cpp
# make -j 2 install

# # bsub -W 2:00 -P BIF128 -nnodes 1 -q batch-hm -Is $SHELL
# # jsrun -n 1 -c 42 -b none make -j 140 install


# # freestanding STL (check out source-only)
# cd $RAPIDS_SRC
# git clone --recurse-submodules https://github.com/rapidsai/thirdparty-freestanding.git

# # cudf python packages
# cd $RAPIDS_SRC/cudf/python/cudf
# PARALLEL_LEVEL=16 CFLAGS=-I$RAPIDS_SRC/thirdparty-freestanding/include python setup.py install
# cd ..
# python -c "import cudf" # test

# cd $RAPIDS_SRC/cudf/python/dask_cudf
# python setup.py install

# # zmq
# cd $RAPIDS_SRC
# git clone https://github.com/zeromq/libzmq.git
# cd libzmq
# export ZMQ_SRC=$PWD
# mkdir -p /tmp/$USER/zmq-build
# cd /tmp/$USER/zmq-build
# cmake -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -D ZMQ_BUILD_TESTS=OFF $ZMQ_SRC
# make -j16 install

# # cppzmq
# cd $RAPIDS_SRC
# git clone https://github.com/zeromq/cppzmq
# cd cppzmq
# export CPPZMQ_SRC=$PWD
# mkdir -p /tmp/$USER/cppzmq-build
# cd /tmp/$USER/cppzmq-build
# cmake -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV $CPPZMQ_SRC
# make -j 8 install


# #
# # JDK / maven
# #

# # download IBM Java 8 SDK
# # go to https://developer.ibm.com/javasdk/downloads/sdk8/
# # under "Linux on Power Systems 64-bit LE", select "Simple unzip with license (InstallAnywhere root not required)"
# # accept license and copy download link
# cd $RAPIDS_SRC
# wget http://public.dhe.ibm.com/ibmdl/export/pub/systems/cloud/runtimes/java/8.0.6.11/linux/ppc64le/ibm-java-sdk-8.0-6.11-ppc64le-archive.bin
# # installation location should be $WORLDWORK/bif128/rapids-env/ibm-java-ppc64le-80
# echo $WORLDWORK/bif128/rapids-env/ibm-java-ppc64le-80
# ./ibm-java-sdk-8.0-6.11-ppc64le-archive.bin
# # confirm prompts

# echo export PATH=$WORLDWORK/bif128/rapids-env/ibm-java-ppc64le-80/bin:\$PATH >> ~/.bashrc
# echo export JAVA_HOME=$WORLDWORK/bif128/rapids-env/ibm-java-ppc64le-80 >> ~/.bashrc
# # ... add 
# source ~/.bashrc

# # Apache maven (binary)
# cd $RAPIDS_SRC
# wget https://downloads.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
# tar xvfz apache-maven-3.6.3-bin.tar.gz
# mv apache-maven-3.6.3 $VIRTUAL_ENV/
# echo export PATH=$WORLDWORK/bif128/envs/rapids/apache-maven-3.6.3/bin:\$PATH >> ~/.bashrc
# source ~/.bashrc

# #
# # blazingSQL
# # have to circumvent the build.sh script, as it expects thirdparty libraries in a special location
# #

# cd $RAPIDS_SRC
# git clone https://github.com/BlazingDB/blazingsql

# # io component
# cd $RAPIDS_SRC
# cd blazingsql/io
# mkdir -p build
# cd build
# CONDA_PREFIX=$VIRTUAL_ENV cmake -D CMAKE_BUILD_TYPE=Release -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV -D BUILD_TESTING=OFF ..
# make -j 16 install

# # comms component
# cd $RAPIDS_SRC/blazingsql/comms
# mkdir -p build
# cd build
# # need to point to cudart.so
# CONDA_PREFIX=$VIRTUAL_ENV cmake -DCMAKE_CXX_STANDARD_LIBRARIES="-L${OLCF_CUDA_ROOT}/lib64" -D CMAKE_BUILD_TYPE=Release -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ..
# make -j 8 install

# # bsql engine
# module load boost/1.66.0
# cd $RAPIDS_SRC/blazingsql/engine
# mkdir -p build
# cd build
# CXXFLAGS="-I${CUDF_HOME}/cpp -L${OLCF_CUDA_ROOT}/lib64 -L$VIRTUAL_ENV/lib" CONDA_PREFIX=$VIRTUAL_ENV cmake -D CMAKE_CXX11_ABI=ON -DBUILD_TESTING=OFF  -D CMAKE_BUILD_TYPE=Release -D CMAKE_INSTALL_PREFIX=$VIRTUAL_ENV ..
# make -j 16 install
 
# # python engine
# cd $RAPIDS_SRC/blazingsql/engine
# CONDA_PREFIX=$VIRTUAL_ENV python setup.py install

# # pyblazing
# cd $RAPIDS_SRC/blazingsql/pyblazing
# python setup.py install 

# # algebra
# cd $RAPIDS_SRC/blazingsql/algebra
# mvn clean install -Dmaven.test.skip=true -f pom.xml -Dmaven.repo.local=$VIRTUAL_ENV/blazing-protocol-mvn/
# cp blazingdb-calcite-application/target/BlazingCalcite.jar $VIRTUAL_ENV/lib/blazingsql-algebra.jar
# cp blazingdb-calcite-core/target/blazingdb-calcite-core.jar $VIRTUAL_ENV/lib/blazingsql-algebra-core.jar

# # pyHIVE (SQL)
# pip install pyhive

# # jpype (java)
# cd $RAPIDS_SRC
# git clone https://github.com/jpype-project/jpype.git
# cd jpype
# python setup.py install
# ln -s $VIRTUAL_ENV/ibm-java-ppc64le-80/jre/lib/ppc64le/compressedrefs/ $VIRTUAL_ENV/lib/server


# # netifaces
# cd $RAPIDS_SRC
# git clone https://github.com/al45tair/netifaces.git
# cd netifaces
# python setup.py install

# # would be great if CONDA_PREFIX was not hardcoded
# export CONDA_PREFIX=$VIRTUAL_ENV

# # need to patch pyblazing
# cd $RAPIDS_SRC/blazingsql
# diff --git a/pyblazing/pyblazing/apiv2/context.py b/pyblazing/pyblazing/apiv2/context.py
# index 1004ce9..727f256 100644
# --- a/pyblazing/pyblazing/apiv2/context.py
# +++ b/pyblazing/pyblazing/apiv2/context.py
# @@ -59,7 +59,8 @@ if not os.path.isfile(jvm_path):
#      # (for newer java versions e.g. 11.x)
#      jvm_path = os.environ["CONDA_PREFIX"] + "/lib/server/libjvm.so"

# -jpype.startJVM("-ea", convertStrings=False, jvmpath=jvm_path)
# +#jpype.startJVM("-ea", convertStrings=False, jvmpath=jvm_path)
# +jpype.startJVM()

#  ArrayClass = jpype.JClass("java.util.ArrayList")
#  ColumnTypeClass = jpype.JClass(

# # (apply the above patch)
# cd pyblazing
# python setup.py install

# # test installation
# python -c "from blazingsql import BlazingContext"