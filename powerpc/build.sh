#!/bin/bash

env_prefix=$1

# NOTE always run this script from blazingsql root folder
blazingsql_project_root_dir=$PWD

set -e

alias python=python3

# NOTE tmp_dir is the prefix (bin, lib, include, build)
#tmp_dir=$working_directory/tmp
# TODO mario
tmp_dir=$env_prefix

# if you want to build in other place just set BLAZINGSQL_POWERPC_TMP_BUILD_DIR before run

if [ -z $BLAZINGSQL_POWERPC_TMP_BUILD_DIR ]; then
  BLAZINGSQL_POWERPC_TMP_BUILD_DIR=/tmp/blazingsql_powerpc_tmp_build_dir/
fi

build_dir=$BLAZINGSQL_POWERPC_TMP_BUILD_DIR

MAKEJ=8
MAKEJ_CUDF=2

export CC=$(which gcc)
export CXX=$(which g++)
export CUDA_HOME=$CUDAPATH
export CUDACXX=$CUDAPATH/bin/nvcc
export BOOST_ROOT=$OLCF_BOOST_ROOT
export LAPACK=$OLCF_NETLIB_LAPACK_ROOT/lib64/liblapack.so
export BLAS=$OLCF_NETLIB_LAPACK_ROOT/lib64/libcblas.so
# NOTE percy mario this var is used by rmm build.sh and by pycudf setup.py
export PARALLEL_LEVEL=$MAKEJ

echo "### Vars ###"
echo "CC="$CC
echo "CXX="$CXX
echo "CUDACXX="$CUDACXX
echo "MAKEJ="$MAKEJ
echo "MAKEJ_CUDF="$MAKEJ_CUDF
echo "PATH="$PATH
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64:/usr/local/nvidia/lib:/usr/local/nvidia/lib64
echo "LD_LIBRARY_PATH="$LD_LIBRARY_PATH
#C_INCLUDE_PATH=/app/tmp/include/
#CPLUS_INCLUDE_PATH=/app/tmp/include/
echo "CPLUS_INCLUDE_PATH="$CPLUS_INCLUDE_PATH
echo "build_dir="$build_dir
echo "blazingsql_project_root_dir=$blazingsql_project_root_dir"

echo "### Pip upgrade ###"
pip install --upgrade pip

echo "### Cython ###"
pip install wheel
pip install cython==0.29.21
mkdir -p $build_dir


#BEGIN zstd
cd $build_dir

if [ ! -d zstd ]; then
    echo "### Zstd - Start ###"
    zstd_version=v1.4.3 # same version used by arrow_version https://github.com/apache/arrow/blob/maint-0.17.x/cpp/thirdparty/versions.txt#L53
    git clone --depth 1 https://github.com/facebook/zstd.git --branch $zstd_version --single-branch
    cd zstd/
    echo "### Zstd - cmake ###"

    cd build/cmake/
    mkdir build
    cd build
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=$tmp_dir ..
    make -j$MAKEJ install
    echo "### Zstd - End ###"
fi

export ZSTD_HOME=$tmp_dir

#END zstd

#BEGIN arrow

cd $build_dir

# NOTE DONT use -e since we need to run some commands after a fail (see arrow)
set +e

function run_cmake_for_arrow() {
    tmp_dir=$1
    cmake \
        -DARROW_BOOST_USE_SHARED=ON \
        -DARROW_BUILD_BENCHMARKS=OFF \
        -DARROW_BUILD_STATIC=OFF \
        -DARROW_BUILD_SHARED=ON \
        -DARROW_BUILD_TESTS=OFF \
        -DARROW_BUILD_UTILITIES=OFF \
        -DARROW_DATASET=ON \
        -DARROW_FLIGHT=OFF \
        -DARROW_GANDIVA=OFF \
        -DARROW_HDFS=OFF \
        -DARROW_JEMALLOC=ON \
        -DARROW_MIMALLOC=ON \
        -DARROW_ORC=ON \
        -DARROW_PARQUET=ON \
        -DARROW_PLASMA=ON \
        -DARROW_PYTHON=ON \
        -DARROW_S3=OFF \
        -DARROW_CUDA=ON \
        -DARROW_SIMD_LEVEL=NONE \
        -DARROW_WITH_BROTLI=ON \
        -DARROW_WITH_BZ2=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_ZSTD=ON \
        -DARROW_USE_LD_GOLD=ON \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_LIBDIR=$tmp_dir/lib \
        -DCMAKE_INSTALL_PREFIX:PATH=$tmp_dir \
        ..
        # TODO Percy check llvm
        #-DLLVM_TOOLS_BINARY_DIR=$PREFIX/bin \
        #-DARROW_DEPENDENCY_SOURCE=SYSTEM \
}

arrow_install_dir=$tmp_dir
echo "arrow_install_dir: "$arrow_install_dir
if [ ! -d arrow ]; then
    echo "### Arrow - start ###"
    arrow_version=apache-arrow-1.0.1
 #   git clone --depth 1 https://github.com/apache/arrow.git --branch $arrow_version --single-branch
    # patched version
    git clone -b fix/power9 --depth 1  https://github.com/williamBlazing/arrow 
    cd arrow/

    echo "### Arrow - cmake ###"
    # NOTE for the arrow cmake arguments:
    # -DARROW_IPC=ON \ # need ipc for blazingdb-ral (because cudf)
    # -DARROW_HDFS=ON \ # blazingdb-io use arrow for hdfs
    # -DARROW_TENSORFLOW=ON \ # enable old ABI for C/C++
    

    cd cpp/
    mkdir -p build
    cd build

    run_cmake_for_arrow $tmp_dir
    
    # NOTE 1) this will fail
    echo "---->>>> BUILD ARROW (1st time)"
    make -j$MAKEJ install
    
    # NOTE 2) we need to apply some patches
    echo "---->>>> APPLY PATCHES TO ARROW"
    cd $build_dir/arrow/cpp/build/orc_ep-prefix/src/orc_ep/cmake_modules/
    ln -s FindZSTD.cmake Findzstd.cmake
    sed -i '1 i\set(ZSTD_STATIC_LIB_NAME libzstd.a)' FindZSTD.cmake

    # NOTE 3) run again
    echo "---->>>> TRY TO BUILD ARROW AGAIN"
    cd $build_dir/arrow/cpp/build
    make -j$MAKEJ install

    echo "### Arrow - end ###"
    
    # BEGIN pyarrow

    export ARROW_HOME=$tmp_dir
    export PARQUET_HOME=$tmp_dir
    #export SETUPTOOLS_SCM_PRETEND_VERSION=$PKG_VERSION
    export PYARROW_BUILD_TYPE=release
    export PYARROW_WITH_DATASET=1
    export PYARROW_WITH_PARQUET=1
    export PYARROW_WITH_ORC=1
    export PYARROW_WITH_PLASMA=1
    export PYARROW_WITH_CUDA=1
    export PYARROW_WITH_GANDIVA=0
    export PYARROW_WITH_FLIGHT=0
    export PYARROW_WITH_S3=0
    export PYARROW_WITH_HDFS=0

    #python -c "import pyarrow"
    #if [ $? != 0 ]; then
        echo "### PyArrow installation ###"
        cd $build_dir/arrow/python
        python setup.py build_ext install --single-version-externally-managed --record=record.txt
    #fi
    echo "pyarrow done"

# END pyarrow
fi


# reenable the error catch system for the shell
set -e

#END arrow


#BEGIN spdlog
cd $build_dir
if [ ! -d spdlog ]; then
    spdlog_version=v1.7.0 # v1.8 has issues for us
    git clone --depth 1 https://github.com/gabime/spdlog.git --branch $spdlog_version --single-branch
    cd spdlog/

    mkdir install
    mkdir build
    cd build

    echo "### Spdlog - cmake ###"
    cmake -DCMAKE_INSTALL_PREFIX=$build_dir/spdlog/install ..

    echo "### Spdlog - make install ###"
    make -j$MAKEJ install

    mkdir -p $env_prefix/include/spdlog
    cp -rf $build_dir/spdlog/install/include/spdlog/* $env_prefix/include/spdlog
    cp -rf $build_dir/spdlog/install/lib64/* $env_prefix/lib64
fi
#END spdlog


cudf_version=0.16

# BEGIN RMM
cd $build_dir
if [ ! -d rmm ]; then
    git clone --depth 1 https://github.com/rapidsai/rmm.git --branch "branch-$cudf_version" --single-branch
    cd rmm
    INSTALL_PREFIX=$tmp_dir CUDACXX=$CUDA_HOME/bin/nvcc ./build.sh  -v clean librmm rmm
fi
# END RMM

# BEGIN DLPACK
cd $build_dir
if [ ! -d dlpack ]; then
    dlpack_version=cudf
    git clone --depth 1 https://github.com/rapidsai/dlpack.git --branch $dlpack_version --single-branch
    cd dlpack
    mkdir myinstall
    mkdir build
    cd build
    cmake -DCMAKE_INSTALL_PREFIX=$build_dir/dlpack/myinstall ..
    make -j$MAKEJ install

    cp -rf $build_dir/dlpack/myinstall/include/* $env_prefix/include/
    cp -rf $build_dir/dlpack/myinstall/lib64/* $env_prefix/lib64
fi
# END DLPACK

# BEGIN CUDF c++
cd $build_dir

export GPU_ARCH="-DGPU_ARCHS=ALL"
export BUILD_NVTX=ON
export BUILD_BENCHMARKS=OFF
export BUILD_DISABLE_DEPRECATION_WARNING=ON
export BUILD_PER_THREAD_DEFAULT_STREAM=OFF
export ARROW_ROOT=$tmp_dir

if [ ! -d cudf ]; then
    cd $build_dir
    echo "### Cudf ###"
   git clone --depth 1 https://github.com/rapidsai/cudf.git --branch "branch-$cudf_version" --single-branch
    cd cudf

    #git submodule update --init --remote --recursive
    #export CUDA_HOME=/usr/local/cuda/
    #export PARALLEL_LEVEL=$build_mode
    #CUDACXX=/usr/local/cuda/bin/nvcc ./build.sh
    #cmake -D GPU_ARCHS=70 -DBUILD_TESTS=ON -DCMAKE_INSTALL_PREFIX=$tmp_dir -DCMAKE_CXX11_ABI=ON ./cpp
    #echo "make"
    #make -j4 install

    cd cpp
    mkdir -p build
    cd build
    cmake -DCMAKE_INSTALL_PREFIX=$tmp_dir \
          -DCMAKE_CXX11_ABI=ON \
          ${GPU_ARCH} \
          -DUSE_NVTX=${BUILD_NVTX} \
          -DBUILD_BENCHMARKS=${BUILD_BENCHMARKS} \
          -DDISABLE_DEPRECATION_WARNING=${BUILD_DISABLE_DEPRECATION_WARNING} \
          -DPER_THREAD_DEFAULT_STREAM=${BUILD_PER_THREAD_DEFAULT_STREAM} \
          -DBOOST_ROOT=$tmp_dir \
          -DBoost_NO_SYSTEM_PATHS=ON \
          -DCMAKE_BUILD_TYPE=Release \
          -DBUILD_TESTS=ON \
          ..
    make -j$MAKEJ_CUDF install
fi

# END CUDF c++

# BEGIN GOLD
cd $build_dir
if [ ! -d binutils ]; then
  # this hash was used to make this work
  #binutils_gdb_gold_linker_ld_version=8d7f06359adf0d3da93acec0f0ded9076f54ebdb
  git clone --depth 1 git://sourceware.org/git/binutils-gdb.git binutils
  cd binutils
  #git checkout $binutils_gdb_gold_linker_ld_version # do not make checkout just build the master branch as is
  ./configure --prefix=$tmp_dir --enable-gold --enable-plugins --disable-werror
  make all-gold -j$MAKEJ
  cp gold/ld-new $tmp_dir/bin/ld
  make -j$MAKEJ
fi
# END GOLD

# NOTE LINKER: replace
cd $build_dir/binutils
cp gold/ld-new $tmp_dir/bin/ld
cp binutils/ar $tmp_dir/bin/ar
cp binutils/nm-new $tmp_dir/bin/nm

# BEGIN LLVM

machine_processor_architecture=`uname -m`

cd $build_dir
if [ ! -d llvm-project ]; then
  llvm_target=""

  if [ "$machine_processor_architecture" = "x86_64" ] || [ "$machine_processor_architecture" = "x86" ]; then
    llvm_target="X86"
  elif [ "$machine_processor_architecture" = "ppc64le" ] || [ "$machine_processor_architecture" = "ppc64" ]; then
    llvm_target="PowerPC"
  fi

  if [ "$llvm_target" = "" ]; then
    echo "ERROR: Unsupported architecture for LLVM"
    exit 0
  fi

  echo "----------------->>> LLVM target: $llvm_target"

  llvm_version=release/9.x
  git clone --single-branch --depth=1 -b $llvm_version https://github.com/llvm/llvm-project.git
  cd llvm-project/llvm/
  wget https://raw.githubusercontent.com/numba/llvmlite/master/conda-recipes/0001-Revert-Limit-size-of-non-GlobalValue-name.patch
  patch -p1 < 0001-Revert-Limit-size-of-non-GlobalValue-name.patch
  mkdir -p build
  cd build
  cmake -D CMAKE_INSTALL_PREFIX=$tmp_dir -DLLVM_ENABLE_PROJECTS=clang -DCMAKE_BUILD_TYPE=Release -DLLVM_TARGETS_TO_BUILD=$llvm_target -D LLVM_BINUTILS_INCDIR=$build_dir/binutils/include/ ../
  make -j1 install
fi

export LLVM_CONFIG=$tmp_dir/bin/llvm-config

# END LLVM

# BEGIN numba llvmlite

# install LLVMgold.so as plugin to 'ar'
cd $build_dir

if [ "$machine_processor_architecture" = "ppc64le" ] || [ "$machine_processor_architecture" = "ppc64" ]; then
  if [ ! -d llvmlite ]; then
    mkdir -p $tmp_dir/lib/bfd-plugins
    cp $tmp_dir/lib/LLVMgold.so $tmp_dir/lib/bfd-plugins
  #  llvmlite_version_from_pip=$(pip show llvmlite |grep Version|awk '{print $2}') # => e.g. 0.33.3
  #  llvmlite_version_from_pip=${llvmlite_version_from_pip%.*} # => e.g. 0.33
    llvmlite_version_from_pip=0.33
    echo "---->>> llvmlite_version_from_pip: $llvmlite_version_from_pip"
    git clone --depth 1 https://github.com/numba/llvmlite.git --branch "release$llvmlite_version_from_pip" --single-branch
    cd llvmlite/
    pip install .

    echo "### Pip dependencies ###"
    # pip install -r requirements.txt
    #pip install --no-binary numpy numpy==1.13.3
    pip install --no-binary numpy numpy
    pip install numba==0.50.1
    # test
    python -c "import numba"

    pip install scikit-learn==0.23.1
    pip install flake8==3.8.3
    pip install ipython==7.17.0
    pip install pytest-timeout==1.4.2
    pip install sphinx-rtd-theme==0.5.0
    pip install cysignals==1.10.2
    pip install numpydoc==1.1.0
    pip install scipy==1.5.2
    pip install pynvml==8.0.4
    pip install cysignals==1.10.2
    pip install networkx==2.4
    pip install jupyterlab==2.2.4
    pip install notebook==6.1.3
    pip install joblib==0.16.0
    pip install fastrlock==0.5
    pip install pytest-timeout==1.4.2
    pip install hypothesis==5.26.0
    pip install python-louvain==0.14
    pip install jupyter-server-proxy==1.5.0
    pip install statsmodels==0.11.1
    pip install pyhive==0.6.2
    pip install thrift==0.13.0
    pip install jpype1==1.0.2
    pip install netifaces==0.10.9
    echo "---->>> finished llvmlite"
  fi
fi

# END numba llvmlite

# NOTE LINKER: restore
cd $tmp_dir/bin
mv ar ar-new
mv nm nm-new
mv ld ld.gold

# FSSPEC
echo "---->>> install fsspec"
pip install --no-binary fsspec fsspec

# BEGIN CUPY
cd $build_dir
if [ ! -d cupy ]; then
    cupy_version=v7.7.0
    git clone --recurse-submodules --depth 1 https://github.com/cupy/cupy.git --branch $cupy_version --single-branch
    cd cupy
    #python setup.py install --user
    pip install .
fi
# END CUPY

export CUDF_ROOT=$build_dir/cudf/cpp/build

# TODO Mario percy the user will need to do this ... for now
# add arrow libs to the lib path for arrow and pycudf
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$tmp_dir/lib

# add g++ libs to the lib path for arrow and pycudf
# TODO: we need to be sure about /usr/local/lib64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64/

# add python libs
# TODO: we need to be sure about /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/

# BEGIN cudf python
if [ ! -d $build_dir/cudf ]; then
    cd $build_dir/cudf/python/cudf
    PARALLEL_LEVEL=$MAKEJ python setup.py build_ext --inplace
    python setup.py install --single-version-externally-managed --record=record.txt 
fi

# END cudf python

dask_version=2.23.0

# BEGIN dask distributed
cd $build_dir
if [ ! -d distributed ]; then
  git clone --depth 1 https://github.com/dask/distributed.git --branch $dask_version --single-branch
  cd distributed
  #python setup.py install --user
  pip install .
fi
# END dask distributed

# BEGIN dask
cd $build_dir
if [ ! -d dask ]; then
  git clone --depth 1 https://github.com/dask/dask.git --branch $dask_version --single-branch
  cd dask
  #python setup.py install --user
  pip install .
fi
# END dask

# BEGIN dask-cuda
cd $build_dir
if [ ! -d dask-cuda ]; then
  git clone --depth 1 https://github.com/rapidsai/dask-cuda.git --branch "branch-$cudf_version" --single-branch
  cd dask-cuda
  #python setup.py install --user
  pip install .
fi
# END dask-cuda

if [ ! -d $build_dir/cudf ]; then
    # BEGIN dask-cudf
    cd $build_dir/cudf/python/dask_cudf
    python setup.py install --single-version-externally-managed --record=record.txt --user
    # END dask-cudf
fi

# BEGIN gtest
# google test
cd $build_dir
if [ ! -d googletest ]; then
  googletest_version=release-1.8.0 # for compatibility with internal gtest in cudf and Apache ORC
  git clone --depth 1 https://github.com/google/googletest.git --branch $googletest_version --single-branch
  cd googletest
  mkdir build
  cd build
  cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=$tmp_dir ..
  make -j$MAKEJ install
fi
# END gtest

# BEGIN zmq

# zmq
cd $build_dir
if [ ! -d libzmq ]; then
  libzmq_version=v4.3.2
  git clone https://github.com/zeromq/libzmq.git
  cd libzmq
  git checkout $libzmq_version
  mkdir build
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=$tmp_dir -D ZMQ_BUILD_TESTS=OFF ..
  make -j$MAKEJ install
fi

# cppzmq
cd $build_dir
if [ ! -d cppzmq ]; then
  cppzmq_version=v4.6.0
  git clone https://github.com/zeromq/cppzmq
  cd cppzmq
  git checkout $cppzmq_version
  mkdir build
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=$tmp_dir ..
  make -j$MAKEJ install
fi

# ENDzmq

# BEGIN blazingsql
cd $blazingsql_project_root_dir
#cd $build_dir
#git clone https://github.com/aucahuasi/blazingsql.git
#cd blazingsql
#git checkout feature/powerpc
#git pull
export INSTALL_PREFIX=$tmp_dir
export BOOST_ROOT=$tmp_dir
export ARROW_ROOT=$tmp_dir
export RMM_ROOT=$tmp_dir
export DLPACK_ROOT=$tmp_dir
export CONDA_PREFIX=$tmp_dir
export CUDF_HOME=$build_dir/cudf/
export THRIFT_INSTALL_DIR=$build_dir/arrow/cpp/build/thrift_ep-install/lib
export SNAPPY_INSTALL_DIR=$build_dir/arrow/cpp/build/snappy_ep/src/snappy_ep-install/lib
export LZ4_INSTALL_DIR=$build_dir/arrow/cpp/build/lz4_ep-prefix/src/lz4_ep/lib

./build.sh -t disable-aws-s3 disable-google-gs

# END blazingsql

echo "FINISH: Your env $env_prefix has blazingsql and cudf"
