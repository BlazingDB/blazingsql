# WARINING nvidia-docker doesn't have powerpc support so this image is only for debugging
FROM nvidia/cuda:10.1-devel-centos7

#COPY install-dependencies.sh /tmp/

RUN yum install wget gcc gcc-c++ bzip2 make libtool python3 openssh-devel -y

#RUN tmp/install-dependencies.sh
#RUN cd /tmp
RUN wget -q http://ftp.mirrorservice.org/sites/sourceware.org/pub/gcc/releases/gcc-7.4.0/gcc-7.4.0.tar.gz
RUN tar -xf gcc-7.4.0.tar.gz
#RUN cd gcc-7.4.0
WORKDIR gcc-7.4.0
RUN ./contrib/download_prerequisites
RUN ./configure --disable-multilib --enable-languages=c,c++ && make -j`nproc` && make install
#RUN rm -rf /tmp/gcc*
RUN mv /usr/bin/gcc /usr/bin/gcc-4
RUN mv /usr/bin/g++ /usr/bin/g++-4
RUN ln -s /usr/local/bin/gcc /usr/bin/
RUN ln -s /usr/local/bin/g++ /usr/bin/
RUN wget -q https://github.com/Kitware/CMake/releases/download/v3.17.3/cmake-3.17.3.tar.gz
RUN tar -xf /tmp/cmake-3.17.3.tar.gz
#RUN cd cmake-3.17.3/
WORKDIR cmake-3.17.3/
RUN ./bootstrap
RUN make
RUN make install
#RUN rm -rf /tmp/cmake
