FROM centos:7
RUN yum install wget gcc gcc-c++ bzip2 make libtool python3 -y && \
    cd /tmp && \
    wget http://ftp.mirrorservice.org/sites/sourceware.org/pub/gcc/releases/gcc-7.4.0/gcc-7.4.0.tar.gz &&\
    tar -xf gcc-7.4.0.tar.gz &&\
    cd gcc-7.4.0 &&\
    ./contrib/download_prerequisites &&\
    ./configure --disable-multilib --enable-languages=c,c++ &&\
    make -j`nproc` &&\
    make install &&\
    rm -rf /tmp/gcc-7.3.0.tar.gz &&\
    mv /usr/bin/gcc /usr/bin/gcc-4 &&\
    mv /usr/bin/g++ /usr/bin/g++-4 &&\
    ln -s /usr/local/bin/gcc /usr/bin/ &&\
    ln -s /usr/local/bin/g++ /usr/bin/

