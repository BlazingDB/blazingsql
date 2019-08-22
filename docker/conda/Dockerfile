ARG  CUDA_VERSION=9.2
ARG  UBUNTU_VERSION=16.04
FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu${UBUNTU_VERSION}

ENV PATH=/miniconda/bin:${PATH}
RUN apt-get update && \
    apt-get install -y git gcc make curl wget && \
    wget -nv -O /tmp/miniconda.sh http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -p /miniconda -b && \
    rm /tmp/miniconda.sh && \
    conda update -y conda && \
    conda install -y conda-build anaconda-client cmake && \
    apt-get clean && \
    conda clean --all
