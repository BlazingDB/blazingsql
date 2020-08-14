FROM centos:7

COPY install-dependencies.sh /tmp/
RUN /tmp/install-dependencies.sh && \
    cd /tmp && \
    wget http://ftp.mirrorservice.org/sites/sourceware.org/pub/gcc/releases/gcc-7.4.0/gcc-7.4.0.tar.gz &&\
    tar -xf gcc-7.4.0.tar.gz &&\
    cd gcc-7.4.0 &&\
    ./contrib/download_prerequisites &&\
    ./configure --disable-multilib --enable-languages=c,c++ &&\
    make -j`nproc` &&\
    make install &&\
    rm -rf /tmp/gcc* &&\
    mv /usr/bin/gcc /usr/bin/gcc-4 &&\
    mv /usr/bin/g++ /usr/bin/g++-4 &&\
    ln -s /usr/local/bin/gcc /usr/bin/ &&\
    ln -s /usr/local/bin/g++ /usr/bin/ &&\
    wget https://github.com/Kitware/CMake/releases/download/v3.17.3/cmake-3.17.3.tar.gz &&\
    tar -xf /tmp/cmake-3.17.3.tar.gz &&\
    cd cmake-3.17.3/ &&\
    ./bootstrap &&\
    make &&\
    make install &&\
    rm -rf /tmp/cmake*
    

