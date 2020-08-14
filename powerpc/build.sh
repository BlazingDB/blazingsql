#!/bin/bash

working_directory=$PWD

output_dir=$working_directory
blazingsql_project_dir=$working_directory/..

if [ ! -z $1 ]; then
  output_dir=$1
fi

# Expand args to absolute/full paths (if the user pass relative paths as args)
output_dir=$(readlink -f $output_dir)
blazingsql_project_dir=$(readlink -f $blazingsql_project_dir)

# NOTE tmp_dir is the prefix (bin, lib, include, build)
tmp_dir=$working_directory/tmp
blazingsql_build_dir=$tmp_dir/build/blazingsql

# Clean the build before start a new one
rm -rf $tmp_dir
mkdir -p $blazingsql_build_dir

# TODO percy mario we need to think better about this
cp -rf $blazingsql_project_dir/algebra $blazingsql_build_dir
cp -rf $blazingsql_project_dir/build.sh $blazingsql_build_dir
cp -rf $blazingsql_project_dir/CHANGELOG.md $blazingsql_build_dir
cp -rf $blazingsql_project_dir/ci $blazingsql_build_dir
cp -rf $blazingsql_project_dir/comms $blazingsql_build_dir
cp -rf $blazingsql_project_dir/conda $blazingsql_build_dir
cp -rf $blazingsql_project_dir/conda-build-docker.sh $blazingsql_build_dir
cp -rf $blazingsql_project_dir/conda_build.jenkinsfile $blazingsql_build_dir
cp -rf $blazingsql_project_dir/CONTRIBUTING.md $blazingsql_build_dir
cp -rf $blazingsql_project_dir/docs $blazingsql_build_dir
cp -rf $blazingsql_project_dir/engine $blazingsql_build_dir
cp -rf $blazingsql_project_dir/io $blazingsql_build_dir
cp -rf $blazingsql_project_dir/LICENSE $blazingsql_build_dir
cp -rf $blazingsql_project_dir/print_env.sh $blazingsql_build_dir
cp -rf $blazingsql_project_dir/pyblazing $blazingsql_build_dir
cp -rf $blazingsql_project_dir/README.md $blazingsql_build_dir
cp -rf $blazingsql_project_dir/scripts $blazingsql_build_dir
cp -rf $blazingsql_project_dir/tests $blazingsql_build_dir
cp -rf $blazingsql_project_dir/test.sh $blazingsql_build_dir
cp -rf $blazingsql_project_dir/thirdparty $blazingsql_build_dir
cp -rf $blazingsql_project_dir/.clang-format $blazingsql_build_dir
cp -rf $blazingsql_project_dir/.git $blazingsql_build_dir
cp -rf $blazingsql_project_dir/.github $blazingsql_build_dir
cp -rf $blazingsql_project_dir/.gitignore $blazingsql_build_dir

cd $blazingsql_build_dir
./build.sh disable-aws-s3 disable-google-gs






#$

exit 1

#BEGIN main


#BEGIN dependencies

if [ $blazingdb_toolchain_clean_before_build == true ]; then
    rm -rf $tmp/blazingdb-toolchain/build/
    rm -rf $tmp/dependencies/
fi

if [ ! -d $tmp/dependencies/include/ ]; then
    echo "## ## ## ## ## ## ## ## Building and installing dependencies ## ## ## ## ## ## ## ##"
    
    mkdir -p $tmp/dependencies/
    
    if [ ! -d $tmp/blazingdb-toolchain/ ]; then
        cd $tmp/
        git clone https://github.com/BlazingDB/blazingdb-toolchain.git
    fi
    
    cd $blazingsql_src_dir/blazingdb-toolchain/
    git checkout $blazingdb_toolchain_branch
    git pull
    
    mkdir -p build
    cd build
    CUDACXX=/usr/local/cuda/bin/nvcc cmake -DCMAKE_INSTALL_PREFIX=$blazingsql_src_dir/dependencies/ ..
    make -j8 install
    
    if [ $? != 0 ]; then
      exit 1
    fi
fi

#BEGIN boost

boost_install_dir=$blazingsql_src_dir/dependencies/

#END boost

#BEGIN rmm
rmm_current_dir=$blazingsql_src_dir/rmm_project/$rmm_branch_name/
rmm_install_dir=$rmm_current_dir/install

if [ $rmm_enable == true ]; then

    echo "### RMM - start ###"

    if [ ! -d $rmm_current_dir ]; then
        mkdir -p $rmm_current_dir
        mkdir -p $rmm_install_dir

        cd $rmm_current_dir
        git clone --recurse-submodules https://github.com/rapidsai/rmm.git
    fi
    
    cd $rmm_current_dir/rmm
    git checkout $rmm_branch
    git pull

    rmm_build_dir=$rmm_current_dir/rmm/build
    mkdir -p $rmm_build_dir
    cd $rmm_build_dir

    echo "### RMM - cmake ###"
    CUDACXX=/usr/local/cuda/bin/nvcc cmake -DCMAKE_BUILD_TYPE=$rmm_build_type \
            -DCMAKE_INSTALL_PREFIX:PATH=$rmm_install_dir \
            ..
    if [ $? != 0 ]; then
      exit 1
    fi

    echo "### RMM - make ###"
    make -j$rmm_parallel install
    if [ $? != 0 ]; then
      exit 1
    fi

    echo "### RMM - end ###"
fi

#END rmm

#BEGIN nvstrings

custrings_install_dir=$blazingsql_src_dir/custrings_project/$custrings_branch_name/install
nvstrings_install_dir=$custrings_install_dir
if [ ! -d $custrings_install_dir ]; then
    custrings_install_dir=""   
    nvstrings_install_dir="" 
fi

if [ $custrings_enable == true ]; then
    custrings_project_dir=$blazingsql_src_dir/custrings_project
    custrings_current_dir=$custrings_project_dir/$custrings_branch_name/
    custrings_dir=cpp
    custrings_build_dir=$custrings_current_dir/custrings/$custrings_dir/build/
    custrings_install_dir=$custrings_current_dir/install
    
    #TODO percy use this path when custrings is part of toolchain dependencies
    #nvstrings_install_dir=$blazingsql_src_dir/dependencies/
    nvstrings_install_dir=$custrings_install_dir
    
    if [ ! -f $custrings_install_dir/include/NVStrings.h ] || [ $custrings_clean_before_build == true ]; then
        #BEGIN custrings
        echo "### Custrings - start ###"
        
        cd $blazingsql_src_dir
        
        if [ ! -d custrings_project ]; then
            mkdir custrings_project
        fi
        
        build_testing_custrings="OFF"
        if [ $custrings_tests == true ]; then
            build_testing_custrings="ON"
        fi
        
        echo "build_testing_custrings: $build_testing_custrings"
        
        cd $custrings_project_dir
        
        # NOTE only for custrings run the cmake command only once so we avoid rebuild everytime 
        if [ ! -d $custrings_branch_name ]; then
            mkdir $custrings_branch_name
            cd $custrings_branch_name
            git clone https://github.com/BlazingDB/custrings.git
            cd $custrings_current_dir/custrings
            git checkout $custrings_branch
            git pull
            git submodule update --init --recursive
            
            echo "### CUSTRINGS - cmake ###"
            mkdir -p $custrings_build_dir
            cd $custrings_build_dir
            CUDACXX=/usr/local/cuda/bin/nvcc RMM_ROOT=$rmm_install_dir cmake -DCMAKE_BUILD_TYPE=$custrings_build_type \
                -DBUILD_TESTS=$build_testing_custrings \
                -DCMAKE_INSTALL_PREFIX:PATH=$custrings_install_dir \
                ..
        fi
        
        cd $custrings_current_dir/custrings
        git checkout $custrings_branch
        git pull
        git submodule update --init --recursive
        
        echo "### CUSTRINGS - clean before build: $custrings_clean_before_build ###"
        
        if [ $custrings_clean_before_build == true ]; then
            rm -rf $custrings_build_dir
            
            echo "### CUSTRINGS - cmake ###"
            mkdir -p $custrings_build_dir
            cd $custrings_build_dir
            CUDACXX=/usr/local/cuda/bin/nvcc RMM_ROOT=$rmm_install_dir cmake -DCMAKE_BUILD_TYPE=$custrings_build_type \
                -DBUILD_TESTS=$build_testing_custrings \
                -DCMAKE_INSTALL_PREFIX:PATH=$custrings_install_dir \
                ..
        fi
        
        echo "### CUSTRINGS - make install ###"
        cd $custrings_build_dir
        make -j$custrings_parallel install
        if [ $? != 0 ]; then
          exit 1
        fi
        
        #END custrings
        
        #TODO percy delete this hack once custrings is installed properly for cudf 0.7
        cp $custrings_install_dir/include/nvstrings/*.h $custrings_install_dir/include/
        
        echo "### Custrings - end ###"
    fi
    
    # Package custrings
    #TODO percy clear these hacks until we migrate to cudf 0.7 properly
    mkdir -p ${output}/nvstrings
    cp -r $custrings_install_dir/* ${output}/nvstrings
    
    if [ $? != 0 ]; then
      exit 1
    fi
    
    mkdir -p ${output}/nvstrings-src
    cp -r $custrings_current_dir/custrings/* ${output}/nvstrings-src
    
    if [ $? != 0 ]; then
      exit 1
    fi
    
    mkdir -p ${output}/nvstrings-build
    cp -r $custrings_current_dir/custrings/cpp/build/* ${output}/nvstrings-build
    
    if [ $? != 0 ]; then
      exit 1
    fi
    
    if [ $? != 0 ]; then
      exit 1
    fi
fi

#END nvstrings

#BEGIN libstd libhdfs3 

libhdfs3_package=libhdfs3
libhdfs3_install_dir=$blazingsql_src_dir/dependencies/$libhdfs3_package

if [ ! -d $libhdfs3_install_dir ]; then
    echo "### Libhdfs3 - start ###"
    cd $blazingsql_src_dir/dependencies/
    libhdfs3_url=https://s3-us-west-2.amazonaws.com/blazing-public-downloads/_libs_/libhdfs3/libhdfs3.tar.gz
    wget $libhdfs3_url
    mkdir $libhdfs3_package
    tar xvf "$libhdfs3_package".tar.gz -C $libhdfs3_package
    if [ $? != 0 ]; then
      exit 1
    fi
    echo "### Libhdfs3 - end ###"
fi

cd $blazingsql_src_dir
mkdir -p $output/$libhdfs3_package/
cp -r $libhdfs3_install_dir/* $output/$libhdfs3_package/

#END libhdfs3

#BEGIN googletest

googletest_install_dir=$blazingsql_src_dir/dependencies/

#END googletest

#BEGIN flatbuffers

flatbuffers_install_dir=$blazingsql_src_dir/dependencies/

#END flatbuffers

#BEGIN lz4

lz4_install_dir=$blazingsql_src_dir/dependencies/

#END lz4

#BEGIN zstd

zstd_install_dir=$blazingsql_src_dir/dependencies/

#END zstd

#BEGIN brotli

brotli_install_dir=$blazingsql_src_dir/dependencies/

#END brotli

#BEGIN snappy

snappy_install_dir=$blazingsql_src_dir/dependencies/

#END snappy

#BEGIN thrift

thrift_install_dir=$blazingsql_src_dir/dependencies/

#END thrift

#BEGIN arrow

arrow_install_dir=$blazingsql_src_dir/dependencies/

#END arrow

#BEGIN aws-sdk-cpp

aws_sdk_cpp_build_dir=$blazingsql_src_dir/dependencies/build/aws-sdk-cpp

#END aws-sdk-cpp

#END dependencies

libgdf_install_dir=$blazingsql_src_dir/cudf_project/$cudf_branch_name/install
if [ ! -d $libgdf_install_dir ]; then
    libgdf_install_dir=""    
fi

if [ $cudf_enable == true ]; then
    #BEGIN cudf
    echo "### Cudf - start ###"
    
    cd $blazingsql_src_dir
    
    if [ ! -d cudf_project ]; then
        mkdir cudf_project
    fi
    
    cudf_project_dir=$blazingsql_src_dir/cudf_project
    cudf_current_dir=$cudf_project_dir/$cudf_branch_name/
    libgdf_dir=cpp
    libgdf_build_dir=$cudf_current_dir/cudf/$libgdf_dir/build/
    libgdf_install_dir=$cudf_current_dir/install
    
    build_testing_cudf="OFF"
    if [ $cudf_tests == true ]; then
        build_testing_cudf="ON"
    fi
    
    echo "build_testing_cudf: $build_testing_cudf"
    
    cd $cudf_project_dir
    
    # NOTE only for cudf run the cmake command only once so we avoid rebuild everytime 
    if [ ! -d $cudf_branch_name ]; then
        mkdir $cudf_branch_name
        cd $cudf_branch_name
        git clone https://github.com/BlazingDB/cudf.git
        cd $cudf_current_dir/cudf
        git checkout $cudf_branch
        git pull
        git submodule update --init --recursive
        
        echo "### CUDF - cmake ###"
        mkdir -p $libgdf_build_dir
        cd $libgdf_build_dir
        BOOST_ROOT=$boost_install_dir CUDACXX=/usr/local/cuda/bin/nvcc NVSTRINGS_ROOT=$nvstrings_install_dir RMM_ROOT=$rmm_install_dir  cmake \
            -DCMAKE_BUILD_TYPE=$cudf_build_type  \
            -DBUILD_TESTS=$build_testing_cudf  \
            -DCMAKE_INSTALL_PREFIX:PATH=$libgdf_install_dir  \
            ..
    fi
    
    cd $cudf_current_dir/cudf
    git checkout $cudf_branch
    git pull
    git submodule update --init --recursive
    
    echo "### CUDF - clean before build: $cudf_clean_before_build ###"
    
    if [ $cudf_clean_before_build == true ]; then
        rm -rf $libgdf_build_dir
        
        echo "### CUDF - cmake ###"
        mkdir -p $libgdf_build_dir
        cd $libgdf_build_dir
        BOOST_ROOT=$boost_install_dir CUDACXX=/usr/local/cuda/bin/nvcc NVSTRINGS_ROOT=$nvstrings_install_dir RMM_ROOT=$rmm_install_dir cmake \
            -DCMAKE_BUILD_TYPE=$cudf_build_type  \
            -DBUILD_TESTS=$build_testing_cudf  \
            -DCMAKE_INSTALL_PREFIX:PATH=$libgdf_install_dir  \
            ..
    fi
    
    echo "### CUDF - make install ###"
    cd $libgdf_build_dir
    make -j$cudf_parallel install
    if [ $? != 0 ]; then
      exit 1
    fi
    
    #END cudf
    
    # Package cudf
    cd $blazingsql_src_dir
    mkdir -p ${output}/cudf/$libgdf_dir/install

    cp -r $cudf_current_dir/cudf/* ${output}/cudf/
    if [ $? != 0 ]; then
      exit 1
    fi

    cp -r $libgdf_install_dir/* ${output}/cudf/$libgdf_dir/install
    if [ $? != 0 ]; then
      exit 1
    fi

    rm -rf ${output}/cudf/.git/
    rm -rf ${output}/cudf/$libgdf_dir/build/

    echo "### Cudf - end ###"
fi


blazingdb_protocol_install_dir=$blazingsql_src_dir/blazingdb-protocol_project/$blazingdb_protocol_branch_name/install
if [ ! -d $blazingdb_protocol_install_dir ]; then
    blazingdb_protocol_install_dir=""    
fi
if [ $blazingdb_protocol_enable == true ]; then
    echo "### Protocol - start ###"
    #BEGIN blazingdb-protocol
    
    cd $blazingsql_src_dir
    
    if [ ! -d blazingdb-protocol_project ]; then
        mkdir blazingdb-protocol_project
    fi
    
    blazingdb_protocol_project_dir=$blazingsql_src_dir/blazingdb-protocol_project
    
    cd $blazingdb_protocol_project_dir
    
    if [ ! -d $blazingdb_protocol_branch_name ]; then
        mkdir $blazingdb_protocol_branch_name
        cd $blazingdb_protocol_branch_name
        git clone https://github.com/BlazingDB/blazingdb-protocol.git
    fi
    
    blazingdb_protocol_current_dir=$blazingdb_protocol_project_dir/$blazingdb_protocol_branch_name/
    
    cd $blazingdb_protocol_current_dir/blazingdb-protocol
    git checkout $blazingdb_protocol_branch
    git pull
    
    cd cpp
    
    blazingdb_protocol_install_dir=$blazingdb_protocol_current_dir/install
    
    blazingdb_protocol_cpp_build_dir=$blazingdb_protocol_current_dir/blazingdb-protocol/cpp/build/
    
    if [ $blazingdb_protocol_clean_before_build == true ]; then
        rm -rf $blazingdb_protocol_cpp_build_dir
    fi
    
    mkdir -p $blazingdb_protocol_cpp_build_dir
    
    cd $blazingdb_protocol_cpp_build_dir
    
    blazingdb_protocol_artifact_name=libblazingdb-protocol.a
    rm -rf lib/$blazingdb_protocol_artifact_name
    
    echo "### Protocol - cmake ###"
    cmake -DCMAKE_BUILD_TYPE=$blazingdb_protocol_build_type \
          -DBLAZINGDB_DEPENDENCIES_INSTALL_DIR=$blazingsql_src_dir/dependencies/ \
          -DCMAKE_INSTALL_PREFIX:PATH=$blazingdb_protocol_install_dir \
          -DCXX_DEFINES=$blazingdb_protocol_definitions \
          ..
    if [ $? != 0 ]; then
      exit 1
    fi

    echo "### Protocol - make install ###"
    make -j$blazingdb_protocol_parallel install
    if [ $? != 0 ]; then
      exit 1
    fi
    
    cd $blazingdb_protocol_current_dir/blazingdb-protocol/java
    echo "### Protocol - mvn ###"
    mvn clean install -Dmaven.test.skip=true
    if [ $? != 0 ]; then
      exit 1
    fi

    blazingdb_protocol_java_build_dir=$blazingdb_protocol_current_dir/blazingdb-protocol/java/target/
    
    #END blazingdb-protocol
    
    # Package blazingdb-protocol/python
    cd $blazingsql_src_dir
    mkdir -p $output/blazingdb-protocol/python/
    cp -r $blazingdb_protocol_current_dir/blazingdb-protocol/python/* $output/blazingdb-protocol/python/

    echo "### Protocol - end ###"
fi

blazingdb_io_install_dir=$blazingsql_src_dir/blazingdb-io_project/$blazingdb_io_branch_name/install
if [ ! -d $blazingdb_io_install_dir ]; then
    blazingdb_io_install_dir=""    
fi
if [ $blazingdb_io_enable == true ]; then
    #BEGIN blazingdb-io
    echo "### Blazingdb IO - start ###"
    
    cd $blazingsql_src_dir
    
    if [ ! -d blazingdb-io_project ]; then
        mkdir blazingdb-io_project
    fi
    
    blazingdb_io_project_dir=$blazingsql_src_dir/blazingdb-io_project
    
    cd $blazingdb_io_project_dir
    
    if [ ! -d $blazingdb_io_branch_name ]; then
        mkdir $blazingdb_io_branch_name
        cd $blazingdb_io_branch_name
        git clone https://github.com/BlazingDB/blazingdb-io.git
    fi
    
    blazingdb_io_current_dir=$blazingdb_io_project_dir/$blazingdb_io_branch_name/
    
    cd $blazingdb_io_current_dir/blazingdb-io
    git checkout $blazingdb_io_branch
    git pull
    
    blazingdb_io_install_dir=$blazingdb_io_current_dir/install
    blazingdb_io_cpp_build_dir=$blazingdb_io_current_dir/blazingdb-io/build/
    
    if [ $blazingdb_io_clean_before_build == true ]; then
        rm -rf $blazingdb_io_cpp_build_dir
    fi
    
    mkdir -p $blazingdb_io_cpp_build_dir
    
    cd $blazingdb_io_cpp_build_dir
    
    blazingdb_io_artifact_name=libblazingdb-io.a
    rm -rf $blazingdb_io_artifact_name
    
    echo "### Blazingdb IO - cmake ###"
    cmake -DCMAKE_BUILD_TYPE=$blazingdb_io_build_type \
          -DBLAZINGDB_DEPENDENCIES_INSTALL_DIR=$blazingsql_src_dir/dependencies/ \
          -DCMAKE_INSTALL_PREFIX:PATH=$blazingdb_io_install_dir \
          -DCXX_DEFINES=$blazingdb_io_definitions \
          ..
    if [ $? != 0 ]; then
      exit 1
    fi

    echo "### Blazingdb IO - make ###"
    make -j$blazingdb_io_parallel install
    if [ $? != 0 ]; then
      exit 1
    fi
    
    echo "### Blazingdb IO - end ###"
    #END blazingdb-io
fi

blazingdb_communication_install_dir=$blazingsql_src_dir/blazingdb-communication_project/$blazingdb_communication_branch_name/install
if [ ! -d $blazingdb_communication_install_dir ]; then
    blazingdb_communication_install_dir=""    
fi
if [ $blazingdb_communication_enable == true ]; then
    #BEGIN blazingdb-communication
    echo "### Blazingdb communication - start ###"
    
    cd $blazingsql_src_dir
    
    if [ ! -d blazingdb-communication_project ]; then
        mkdir blazingdb-communication_project
    fi
    
    blazingdb_communication_project_dir=$blazingsql_src_dir/blazingdb-communication_project
    
    cd $blazingdb_communication_project_dir
    
    if [ ! -d $blazingdb_communication_branch_name ]; then
        mkdir $blazingdb_communication_branch_name
        cd $blazingdb_communication_branch_name
        git clone https://github.com/BlazingDB/blazingdb-communication.git
    fi
    
    blazingdb_communication_current_dir=$blazingdb_communication_project_dir/$blazingdb_communication_branch_name/
    
    cd $blazingdb_communication_current_dir/blazingdb-communication
    git checkout $blazingdb_communication_branch
    git pull
    
    blazingdb_communication_install_dir=$blazingdb_communication_current_dir/install
    blazingdb_communication_cpp_build_dir=$blazingdb_communication_current_dir/blazingdb-communication/build/
    
    if [ $blazingdb_communication_clean_before_build == true ]; then
        rm -rf $blazingdb_communication_cpp_build_dir
    fi
    
    mkdir -p $blazingdb_communication_cpp_build_dir
    
    cd $blazingdb_communication_cpp_build_dir
    
    blazingdb_communication_artifact_name=libblazingdb-communication.a
    rm -rf $blazingdb_communication_artifact_name
    
    echo "### Blazingdb communication - cmake ###"
    cmake -DCMAKE_BUILD_TYPE=$blazingdb_communication_build_type \
          -DBLAZINGDB_DEPENDENCIES_INSTALL_DIR=$blazingsql_src_dir/dependencies/ \
          -DCMAKE_INSTALL_PREFIX:PATH=$blazingdb_communication_install_dir \
          -DCUDA_DEFINES=$blazingdb_communication_definitions \
          -DCXX_DEFINES=$blazingdb_communication_definitions \
          ..
    if [ $? != 0 ]; then
      exit 1
    fi

    echo "### Blazingdb communication - make ###"
    make -j$blazingdb_communication_parallel install
    if [ $? != 0 ]; then
      exit 1
    fi
    
    echo "### Blazingdb communication - end ###"
    #END blazingdb-communication
fi

if [ $blazingdb_ral_enable == true ]; then
    #BEGIN blazingdb-ral
    echo "### Ral - start ###"
    
    cd $blazingsql_src_dir
    
    if [ ! -d blazingdb-ral_project ]; then
        mkdir blazingdb-ral_project
    fi
    
    blazingdb_ral_project_dir=$blazingsql_src_dir/blazingdb-ral_project
    
    cd $blazingdb_ral_project_dir
    
    if [ ! -d $blazingdb_ral_branch_name ]; then
        mkdir $blazingdb_ral_branch_name
        cd $blazingdb_ral_branch_name
        git clone https://github.com/BlazingDB/blazingdb-ral.git
    fi
    
    blazingdb_ral_current_dir=$blazingdb_ral_project_dir/$blazingdb_ral_branch_name/
    
    cd $blazingdb_ral_current_dir/blazingdb-ral
    git checkout $blazingdb_ral_branch
    git pull
    git submodule update --init --recursive
    
    blazingdb_ral_install_dir=$blazingdb_ral_current_dir/install
    blazingdb_ral_build_dir=$blazingdb_ral_current_dir/blazingdb-ral/build/
    
    if [ $blazingdb_ral_clean_before_build == true ]; then
        rm -rf $blazingdb_ral_build_dir
    fi
    
    mkdir -p $blazingdb_ral_build_dir
    
    cd $blazingdb_ral_build_dir
    
    #TODO fix the artifacts name
    blazingdb_ral_artifact_name=testing-libgdf
    rm -rf $blazingdb_ral_artifact_name
    
    build_testing_ral="OFF"
    if [ $blazingdb_ral_tests == true ]; then
        build_testing_ral="ON"
    fi
    
    echo "### Ral - cmake ###"
    
    # Configure blazingdb-ral with dependencies
    CUDACXX=/usr/local/cuda/bin/nvcc cmake -DCMAKE_BUILD_TYPE=$blazingdb_ral_build_type \
          -DBUILD_TESTING=$build_testing_ral \
          -DBLAZINGDB_DEPENDENCIES_INSTALL_DIR=$blazingsql_src_dir/dependencies/ \
          -DRMM_INSTALL_DIR=$rmm_install_dir \
          -DNVSTRINGS_INSTALL_DIR=$nvstrings_install_dir/ \
          -DLIBGDF_INSTALL_DIR=$libgdf_install_dir \
          -DBLAZINGDB_PROTOCOL_INSTALL_DIR=$blazingdb_protocol_install_dir \
          -DBLAZINGDB_IO_INSTALL_DIR=$blazingdb_io_install_dir \
          -DBLAZINGDB_COMMUNICATION_INSTALL_DIR=$blazingdb_communication_install_dir \
          -DCUDA_DEFINES=$blazingdb_ral_definitions \
          -DCXX_DEFINES=$blazingdb_ral_definitions \
          ..
    if [ $? != 0 ]; then
      exit 1
    fi

    echo "### Ral - make ###"
    make -j$blazingdb_ral_parallel
    if [ $? != 0 ]; then
      exit 1
    fi
    
    #END blazingdb-ral
    
    # Package blazingdb-ral
    cd $blazingsql_src_dir
    blazingdb_ral_artifact_name=testing-libgdf
    cp $blazingdb_ral_build_dir/$blazingdb_ral_artifact_name $output
    if [ $? != 0 ]; then
      exit 1
    fi

    echo "### Ral - end ###"
fi

if [ $blazingdb_orchestrator_enable == true ]; then
    #BEGIN blazingdb-orchestrator
    echo "### Orchestrator - start ###"
    
    cd $blazingsql_src_dir
    
    if [ ! -d blazingdb-orchestrator_project ]; then
        mkdir blazingdb-orchestrator_project
    fi
    
    blazingdb_orchestrator_project_dir=$blazingsql_src_dir/blazingdb-orchestrator_project
    
    cd $blazingdb_orchestrator_project_dir
    
    if [ ! -d $blazingdb_orchestrator_branch_name ]; then
        mkdir $blazingdb_orchestrator_branch_name
        cd $blazingdb_orchestrator_branch_name
        git clone https://github.com/BlazingDB/blazingdb-orchestrator.git
    fi
    
    blazingdb_orchestrator_current_dir=$blazingdb_orchestrator_project_dir/$blazingdb_orchestrator_branch_name/
    
    cd $blazingdb_orchestrator_current_dir/blazingdb-orchestrator
    git checkout $blazingdb_orchestrator_branch
    git pull
    
    blazingdb_orchestrator_install_dir=$blazingdb_orchestrator_current_dir/install
    blazingdb_orchestrator_build_dir=$blazingdb_orchestrator_current_dir/blazingdb-orchestrator/build/
    
    if [ $blazingdb_orchestrator_clean_before_build == true ]; then
        rm -rf $blazingdb_orchestrator_build_dir
    fi
    
    mkdir -p $blazingdb_orchestrator_build_dir
    cd $blazingdb_orchestrator_build_dir
    
    #TODO fix the artifacts name
    blazingdb_orchestrator_artifact_name=blazingdb_orchestator_service
    rm -f $blazingdb_orchestrator_artifact_name
    
    # TODO percy FIX orchestrator
    #-DBLAZINGDB_PROTOCOL_INSTALL_DIR=$blazingdb_protocol_install_dir \
    # -DFLATBUFFERS_INSTALL_DIR=$flatbuffers_install_dir \
    # -DGOOGLETEST_INSTALL_DIR=$googletest_install_dir \
    echo "### Orchestrator - cmake ###"
    cmake -DCMAKE_BUILD_TYPE=$blazingdb_orchestrator_build_type  \
          -DBLAZINGDB_DEPENDENCIES_INSTALL_DIR=$blazingsql_src_dir/dependencies/ \
          -DBLAZINGDB_PROTOCOL_INSTALL_DIR=$blazingdb_protocol_install_dir \
          -DBLAZINGDB_COMMUNICATION_INSTALL_DIR=$blazingdb_communication_install_dir \
          -DCUDA_DEFINES=$blazingdb_orchestrator_definitions \
          -DCXX_DEFINES=$blazingdb_orchestrator_definitions \
          ..
    if [ $? != 0 ]; then
      exit 1
    fi

    echo "### Orchestrator - make ###"
    make -j$blazingdb_orchestrator_parallel
    if [ $? != 0 ]; then
      exit 1
    fi
    
    #END blazingdb-orchestrator
    
    # Package blazingdb-orchestrator
    cd $blazingsql_src_dir
    blazingdb_orchestrator_artifact_name=blazingdb_orchestator_service
    cp $blazingdb_orchestrator_build_dir/$blazingdb_orchestrator_artifact_name $output

    echo "### Orchestrator - end ###"
fi

if [ $blazingdb_calcite_enable == true ]; then
    #BEGIN blazingdb-calcite
    echo "### Calcite - start ###"
    
    cd $blazingsql_src_dir
    
    if [ ! -d blazingdb-calcite_project ]; then
        mkdir blazingdb-calcite_project
    fi
    
    blazingdb_calcite_project_dir=$blazingsql_src_dir/blazingdb-calcite_project
    
    cd $blazingdb_calcite_project_dir
    
    if [ ! -d $blazingdb_calcite_branch_name ]; then
        mkdir $blazingdb_calcite_branch_name
        cd $blazingdb_calcite_branch_name
        git clone https://github.com/BlazingDB/blazingdb-calcite.git
    fi
    
    blazingdb_calcite_current_dir=$blazingdb_calcite_project_dir/$blazingdb_calcite_branch_name/
    
    cd $blazingdb_calcite_current_dir/blazingdb-calcite
    git checkout $blazingdb_calcite_branch
    git pull
    
    blazingdb_calcite_install_dir=$blazingdb_calcite_current_dir/install
    
    echo "### Calcite - mvn clean install ###"
    mvn clean install -Dmaven.test.skip=true
    if [ $? != 0 ]; then
      exit 1
    fi

    blazingdb_calcite_build_dir=$blazingdb_calcite_current_dir/blazingdb-calcite/blazingdb-calcite-application/target/
    
    #END blazingdb-calcite
    
    # Package blazingdb-calcite
    cd $blazingsql_src_dir
    blazingdb_calcite_artifact_name=BlazingCalcite.jar
    cp $blazingdb_calcite_build_dir/$blazingdb_calcite_artifact_name ${output}

    echo "### Calcite - end ###"
fi

if [ $pyblazing_enable == true ]; then
    #BEGIN pyblazing
    echo "### Pyblazing - start ###"
    
    cd $blazingsql_src_dir
    
    if [ ! -d pyblazing_project ]; then
        mkdir pyblazing_project
    fi
    
    pyblazing_project_dir=$blazingsql_src_dir/pyblazing_project
    
    cd $pyblazing_project_dir
    
    if [ ! -d $pyblazing_branch_name ]; then
        mkdir $pyblazing_branch_name
        cd $pyblazing_branch_name
        git clone https://github.com/BlazingDB/pyBlazing.git
    fi
    
    pyblazing_current_dir=$pyblazing_project_dir/$pyblazing_branch_name/
    
    cd $pyblazing_current_dir/pyBlazing
    git checkout $pyblazing_branch
    git pull
    
    pyblazing_install_dir=$pyblazing_current_dir/install
    
    #END pyblazing
    
    # Package PyBlazing
    cd $blazingsql_src_dir
    mkdir -p ${output}/pyBlazing/
    cp -r $pyblazing_current_dir/pyBlazing/* ${output}/pyBlazing/
    if [ $? != 0 ]; then
      exit 1
    fi

    rm -rf ${output}/pyBlazing/.git/
    echo "### Pyblazing - end ###"
fi

# Final step: compress files and delete temp folder

cd $output_dir && tar czf blazingsql-files.tar.gz blazingsql-files/

if [ -d $output ]; then
    echo "###################### BUILT STATUS #####################"
    if [ $blazingdb_ral_enable == true ]; then
        if [ -f $output/testing-libgdf ]; then
            echo "RAL - built OK."
        else
            echo "RAL - compiled with errors."
        fi
    fi

    if [ $blazingdb_orchestrator_enable == true ]; then
        if [ -f $output/blazingdb_orchestator_service ]; then
            echo "ORCHESTRATOR - built OK."
        else
            echo "ORCHESTRATOR - compiled with errors."
        fi
    fi

    if [ $blazingdb_calcite_enable == true ]; then
        if [ -f $output/BlazingCalcite.jar ]; then
            echo "CALCITE - built OK."
        else
            echo "CALCITE - compiled with errors."
        fi
    fi
fi

rm -rf ${output}

cd $working_directory

echo "######################## SUMMARY ########################"

if [ $cudf_enable == true ]; then
    echo "CUDF: "
    cudf_dir=$blazingsql_src_dir/cudf_project/$cudf_branch_name/cudf
    cd $cudf_dir
    cudf_commit=$(git log | head -n 1)
    echo '      '$cudf_commit
    echo '      '"branch "$cudf_branch_name
fi

if [ $blazingdb_protocol_enable == true ]; then
    echo "PROTOCOL: "
    protocol_dir=$blazingsql_src_dir/blazingdb-protocol_project/$blazingdb_protocol_branch_name/blazingdb-protocol
    cd $protocol_dir
    protocol_commit=$(git log | head -n 1)
    echo '      '$protocol_commit
    echo '      '"branch "$blazingdb_protocol_branch_name
fi

if [ $blazingdb_io_enable == true ]; then
    echo "BLAZING-IO: "
    io_dir=$blazingsql_src_dir/blazingdb-io_project/$blazingdb_io_branch_name/blazingdb-io
    cd $io_dir
    io_commit=$(git log | head -n 1)
    echo '      '$io_commit
    echo '      '"branch "$blazingdb_io_branch_name
fi

if [ $blazingdb_communication_enable == true ]; then
    echo "COMMUNICATION: "
    communication_dir=$blazingsql_src_dir/blazingdb-communication_project/$blazingdb_communication_branch_name/blazingdb-communication
    cd $communication_dir
    communication_commit=$(git log | head -n 1)
    echo '      '$communication_commit
    echo '      '"branch "$blazingdb_communication_branch_name
fi

if [ $blazingdb_ral_enable == true ]; then
    echo "RAL: "
    ral_dir=$blazingsql_src_dir/blazingdb-ral_project/$blazingdb_ral_branch_name/blazingdb-ral
    cd $ral_dir
    ral_commit=$(git log | head -n 1)
    echo '      '$ral_commit
    echo '      '"branch "$blazingdb_ral_branch_name
fi

if [ $blazingdb_orchestrator_enable == true ]; then
    echo "ORCHESTRATOR: "
    orch_dir=$blazingsql_src_dir/blazingdb-orchestrator_project/$blazingdb_orchestrator_branch_name/blazingdb-orchestrator
    cd $orch_dir
    orch_commit=$(git log | head -n 1)
    echo '      '$orch_commit
    echo '      '"branch "$blazingdb_orchestrator_branch_name
fi

if [ $blazingdb_calcite_enable == true ]; then
    echo "CALCITE: "
    calcite_dir=$blazingsql_src_dir/blazingdb-calcite_project/$blazingdb_calcite_branch_name/blazingdb-calcite
    cd $calcite_dir
    calcite_commit=$(git log | head -n 1)
    echo '      '$calcite_commit
    echo '      '"branch "$blazingdb_calcite_branch_name
fi

if [ $pyblazing_enable == true ]; then
    echo "PYBLAZING: "
    pyblazing_dir=$blazingsql_src_dir/pyblazing_project/$pyblazing_branch_name/pyBlazing
    cd $pyblazing_dir
    pyblazing_commit=$(git log | head -n 1)
    echo '      '$pyblazing_commit
    echo '      '"branch "$pyblazing_branch_name
fi

echo "##########################################################"

#END main





#####################################################

RAPIDS 0.14

Setting Conda Environment

module load gcc/7.4.0
module load cmake/3.15.2
module load python/3.7.0-anaconda3-5.3.0
module load cuda/10.1.243

conda create -p /gpfs/alpine/world-shared/stf011/nvrapids_0.14_gcc_7.4.0 python=3.7
source activate /gpfs/alpine/world-shared/stf011/nvrapids_0.14_gcc_7.4.0

export PATH=$CUDA_DIR/bin:$PATH
export LD_LIBRARY_PATH=$CONDA_PREFIX/nvvm/lib64:$CONDA_PREFIX/lib:$LD_LIBRARY_PATH
export CC=/sw/summit/gcc/7.4.0/bin/gcc
export CXX=/sw/summit/gcc/7.4.0/bin/g++
export PARALLEL_LEVEL=8

Install General Dependencies

conda install -c conda-forge numba=0.49.0 pandas=0.25.3 fastavro=0.23.5 cython=0.29.20 fsspec=0.7.4 pytest=5.4.3 scikit-learn=0.23.1 umap-learn=0.4.4 protobuf=3.12.3 pytest-timeout sphinx_rtd_theme numpydoc notebook dask=2.19.0 distributed=2.19.0 rapidjson flatbuffers double-conversion boost-cpp hypothesis ipython flake8 snappy orc  networkx python-louvain dlpack jupyter-server-proxy dask_labextension scipy statsmodels numpy jupyterlab nodejs pynvml libhwloc boost cysignals joblib fastrlock jupyterlab-nvdashboard ncurses -y

pip install cmake_setuptools 
jupyter labextension install dask-labextension jupyterlab-nvdashboard


Install CUDA 10.2 (required by libcumlprims)

wget https://public.dhe.ibm.com/ibmdl/export/pub/software/server/ibm-ai/conda/linux-ppc64le/cudatoolkit-dev-10.2.89-680.g0f7a43a.tar.bz2

conda install cudatoolkit-dev-10.2.89-680.g0f7a43a.tar.bz2

export CUDA_DIR=$CONDA_PREFIX
export CUDA_TOOLKIT_LIB_PATH=$CONDA_PREFIX/cuda/lib

Build/Install RMM

git clone --single-branch --branch branch-0.14 --recurse-submodules https://github.com/rapidsai/rmm.git

cd rmm

./build.sh librmm
./build.sh rmm

Build/Install cuDF

Build/Install Thrift 0.11

git clone --branch 0.11.0 https://github.com/apache/thrift.git
cd thrift
./thrift.sh

Build/Install Arrow/PyArrow 0.15

git clone https://github.com/apache/arrow.git

cd arrow
#switch to tag apache-arrow-0.15.0
git checkout apache-arrow-0.15.0
./arrow.sh
./pyarrow.sh
cd ..

Build/Install cuDF (build.sh modified to use our  Arrow)

git clone --single-branch --branch branch-0.14 --recurse-submodules https://github.com/rapidsai/cudf.git

Patch cudf/thirdparty/libcudacxx/include/simt/cfloat  with the next constants for Power9

#define __LDBL_MAX__ 1.79769313486231580793728971405301e+308L
#define __LDBL_MIN__ 2.00416836000897277799610805135016e-292L
#define __LDBL_EPSILON__ 4.94065645841246544176568792868221e-324L
#define __LDBL_DENORM_MIN__ 4.94065645841246544176568792868221e-324L


cd cudf
./build.sh

Build/Install Dask-CUDA

git clone https://github.com/rapidsai/dask-cuda.git
cd dask-cuda
#move to tag v0.14.1
git checkout v0.14.1
python setup.py build_ext --inplace
pip install .

Build/Install cuML

Install LLVM and clang 8.0.1 in $CONDA_PREFIX

wget https://github.com/llvm/llvm-project/releases/download/llvmorg-8.0.1/clang+llvm-8.0.1-powerpc64le-linux-rhel-7.4.tar.xz

tar -xvf clang+llvm-8.0.1-powerpc64le-linux-rhel-7.4.tar.xz
cd clang+llvm-8.0.1-powerpc64le-linux-rhel-7.4
cp -r * $CONDA_PREFIX

Build/Install NCCL 2.4

git clone https://github.com/NVIDIA/nccl.git
cd nccl
git checkout v2.7.8-1

build with https://github.com/benjha/nvrapids_olcf/blob/branch-0.11/build_scripts/nccl/nccl.sh

Build/Install UCX >= 1.7

git clone --branch=v1.8.x https://github.com/openucx/ucx.git
Build using 
https://github.com/benjha/nvrapids_olcf/blob/branch-0.11/build_scripts/ucx/ucx.sh

Build/Install UCX-py

git clone --single-branch --branch branch-0.14 https://github.com/rapidsai/ucx-py.git

cd ucx-py
python setup.py build_ext --inplace
pip install .


Build/Install cuML

git clone --single-branch --branch branch-0.14 --recurse-submodules https://github.com/rapidsai/cuml.git
cd cuML
./build.sh libcuml

get and install libcumlprims

cd $CONDA_PREFIX
wget https://public.dhe.ibm.com/ibmdl/export/pub/software/server/ibm-ai/conda-early-access/linux-ppc64le/libcumlprims-0.14.0a-640.gf76300c.tar.bz2
tar -xvf libcumlprims-0.14.0a-640.gf76300c.tar.bz2

./build.sh cuml

Build/Install XGBoost

git clone --single-branch --branch rapids-0.14-release --recursive https://github.com/rapidsai/xgboost.git

mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX -DUSE_CUDA=ON -DUSE_NCCL=ON -DNCCL_ROOT=$CONDA_PREFIX -DGPU_COMPUTE_VER=70
cd ../python-package
pip install -e

Build/Install Dask-XGBoost

git clone https://github.com/rapidsai/dask-xgboost.git
cd dask-xgboost
git checkout v2.7.8-1

Build/Install CuPy  7.5.0

Install cuTENSOR

mkdir cutensor
cd cutensor

#gets header files
wget https://developer.download.nvidia.com/compute/cuda/repos/rhel7/ppc64le/libcutensor-devel-1.0.1-1.ppc64le.rpm

#gets dynamic libraries
wget https://developer.download.nvidia.com/compute/cuda/repos/rhel7/ppc64le/libcutensor1-1.0.1-1.ppc64le.rpm

rpm2cpio libcutensor-devel-1.0.1-1.ppc64le.rpm | cpio -idmv
rpm2cpio libcutensor1-1.0.1-1.ppc64le.rpm | cpio -idmv

cp includes and libs to $CONDA_PREFIX/include and $CONDA_PREFIX/lib

Install CUB 1.8.0

wget https://github.com/NVlabs/cub/archive/1.8.0.zip
unzip 1.8.0.zip
cd cub-1.8.0
cp -r cub $CONDA_PREFIX

Build/Install CuPy

git clone https://github.com/cupy/cupy.git
cd cupy
git checkout v7.5.0

export CUB_PATH=$CONDA_PREFIX
export CUTENSOR_PATH=$CONDA_PREFIX

python setup.py build
python setup.py install


Build/Install cuGraph 

git clone --single-branch --branch branch-0.14 https://github.com/rapidsai/cuml.git


Build Install peg/leg

wget https://www.piumarta.com/software/peg/peg-0.1.18.tar.gz
tar -xzvf peg-0.1.18.tar.gz
cd peg-0.1.18 

#Patch Makefile
sed -i 's/\/usr\/local/$(CONDA_PREFIX)/g'  Makefile
make 
make install prefix=$CONDA_PREFIX

LIBcypher-parser

./autogen.sh
./configure --prefix=$CONDA_PREFIX
make -j${PARALLEL_LEVEL}
make install
