#!/bin/bash

# 1) hsi/5.0.2.p5    4) darshan-runtime/3.1.7   7) cuda/10.1.243  10) spectrum-mpi/10.3.1.2-20200121
# 2) xalt/1.2.0      5) DefApps                 8) gcc/7.4.0      11) boost/1.66.0
# 3) lsf-tools/2.0   6) python/3.7.0            9) cmake/3.17.3

#BEGIN boost

tmp_dir=$1

boost_install_dir=$tmp_dir
boost_build_dir=$build_dir/boost/
echo "boost_install_dir: "$boost_install_dir
echo "boost_build_dir: "$boost_build_dir

# TODO percy mario manage version labels (1.66.0 and 1_66_0)
if [ ! -f $boost_install_dir/include/boost/version.hpp ]; then
    echo "### Boost - start ###"
    mkdir -p $boost_build_dir
    cd $boost_build_dir

    # TODO percy mario manbage boost version
    if [ ! -f $boost_build_dir/boost_1_66_0.tar.gz ]; then
        wget -q https://dl.bintray.com/boostorg/release/1.66.0/source/boost_1_66_0.tar.gz
    fi

    if [ ! -d $boost_build_dir/boost_1_66_0 ]; then
        echo "Decompressing boost_1_66_0.tar.gz ..."
        tar xf boost_1_66_0.tar.gz
        echo "Boost package boost_1_66_0.tar.gz was decompressed at $boost_dir"
    fi

    # NOTE build Boost with -fPIC
    cd boost_1_66_0
    ./bootstrap.sh --with-libraries=system,filesystem,regex,atomic,chrono,container,context,thread --with-icu --prefix=$boost_install_dir
    ./b2 install variant=release stage threading=multi --exec-prefix=$boost_install_dir --prefix=$boost_install_dir -a
    if [ $? != 0 ]; then
      echo "Error during b2 install"
      exit 1
    fi
    echo "### Boost - end ###"
fi
#END boost
