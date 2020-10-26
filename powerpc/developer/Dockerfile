# WARINING nvidia-docker doesn't have powerpc support so this image is only for debugging
FROM nvidia/cuda:10.1-devel-centos7

#COPY install-dependencies.sh /tmp/

RUN yum install wget gcc gcc-c++ bzip2 make libtool openssh-devel zlib-devel -y

#RUN tmp/install-dependencies.sh
#RUN cd /tmp

#### GCC/G++ ######
RUN wget -q http://ftp.mirrorservice.org/sites/sourceware.org/pub/gcc/releases/gcc-7.4.0/gcc-7.4.0.tar.gz
RUN tar -xf gcc-7.4.0.tar.gz
#RUN cd gcc-7.4.0
WORKDIR gcc-7.4.0
RUN ./contrib/download_prerequisites
RUN ./configure --disable-multilib --enable-languages=c,c++ && make -j`nproc` && make install
#RUN rm -rf /tmp/gcc*
RUN update-alternatives --install /usr/bin/cc cc /usr/local/bin/gcc 999
RUN rm -f /usr/bin/gcc
RUN update-alternatives --install /usr/bin/gcc gcc /usr/local/bin/gcc 999
RUN rm -f /usr/bin/g++
RUN update-alternatives --install /usr/bin/g++ g++ /usr/local/bin/g++ 999
RUN echo '/usr/local/lib' > /etc/ld.so.conf.d/local-lib.conf
RUN echo '/usr/local/lib64' > /etc/ld.so.conf.d/local-lib64.conf
ENV PATH=${PATH}:/opt/blazingsql-powerpc-prefix/bin/
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64:/usr/local/nvidia/lib:/usr/local/nvidia/lib64
ENV CC=/usr/local/bin/gcc
ENV CXX=/usr/local/bin/g++
ENV CUDACXX=/usr/local/cuda/bin/nvcc

#### CMAKE ####
RUN yum install openssl-devel -y
RUN wget -q https://github.com/Kitware/CMake/releases/download/v3.17.3/cmake-3.17.3.tar.gz
RUN tar -xf cmake-3.17.3.tar.gz
#RUN cd cmake-3.17.3/
WORKDIR cmake-3.17.3/
RUN ./bootstrap
RUN make -j`nproc`
RUN make install
#RUN rm -rf /tmp/cmake
ENV CMAKE_ROOT=/usr/local/share/cmake-3.17/

#### Python ######
RUN yum install -y libffi-devel zlib zlib-devel
RUN yum install -y bzip2-devel
RUN yum install -y xz-devel
RUN wget -q https://www.python.org/ftp/python/3.7.7/Python-3.7.7.tgz
RUN tar -xf Python-3.7.7.tgz
RUN cd Python-3.7.7/ && ./configure --enable-optimizations --enable-shared && make altinstall
RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.7 999

WORKDIR /app

# TODO percy mario install more utils
RUN yum install git nano -y

# Dependency for boost
RUN yum install which -y

# Dependency for scikit-learn
RUN yum install epel-release -y
RUN yum install -y openblas-devel

# LLVM
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum install -y texinfo
#ENV LLVM_CONFIG=/usr/bin/llvm-config-9.0-64

# Python packages
RUN yum install -y libjpeg-devel freetype-devel
RUN mkdir -p /opt/blazingsql-powerpc-prefix
COPY requirements.txt /opt/requirements.txt
RUN python3 -m venv /opt/blazingsql-powerpc-prefix
RUN source /opt/blazingsql-powerpc-prefix/bin/activate && pip3.7 install -r /opt/requirements.txt

# For blazingsql
RUN yum install -y libcurl-devel
RUN yum install -y maven

# lmod (aka system dependencies)
COPY install-dependencies.sh /opt/install-dependencies.sh
RUN bash /opt/install-dependencies.sh /opt/blazingsql-powerpc-prefix

# Lmod
RUN yum install -y Lmod
RUN mkdir -p /opt/sw/
COPY modulefiles /opt/modulefiles
ENV MODULEPATH=/opt/modulefiles/

WORKDIR /app

# More utils
RUN yum install -y tree

RUN chmod 777 /opt
RUN chmod 777 /opt/blazingsql-powerpc-prefix
RUN chmod 777 /opt/blazingsql-powerpc-prefix/bin
RUN chmod 777 /opt/blazingsql-powerpc-prefix/lib
#RUN chmod 777 /opt/blazingsql-powerpc-prefix/lib/pkgconfig/
RUN chmod 777 /opt/blazingsql-powerpc-prefix/lib/python3.7/
RUN chmod 777 /opt/blazingsql-powerpc-prefix/lib/python3.7/site-packages/
RUN chmod 777 /opt/blazingsql-powerpc-prefix/lib64
#RUN chmod 777 /opt/blazingsql-powerpc-prefix/lib64/pkgconfig/
RUN chmod 777 /opt/blazingsql-powerpc-prefix/include
RUN chmod 777 /opt/blazingsql-powerpc-prefix/etc
RUN chmod 777 /opt/blazingsql-powerpc-prefix/share
RUN chmod 777 /opt/blazingsql-powerpc-prefix/share/cysignals/
RUN chmod 777 /opt/blazingsql-powerpc-prefix/share/doc/
RUN chmod 777 /opt/blazingsql-powerpc-prefix/share/jupyter/
RUN chmod 777 /opt/blazingsql-powerpc-prefix/share/man/
RUN chmod 777 /opt/blazingsql-powerpc-prefix/share/man/man1

# More deps for llvm
RUN yum install -y patch
