ARG  CUDA_VERSION=9.2
ARG  UBUNTU_VERSION=16.04
FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu${UBUNTU_VERSION}

ENV PATH=${PATH}:/miniconda3/bin
RUN apt-get update && \
    apt-get install -y git gcc make curl wget && \
    wget -q -O /tmp/miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -bfp /miniconda3/ && \
    rm -f /tmp/miniconda.sh && \
    conda update -y conda && \
    conda install -y conda-build anaconda-client cmake && \
    apt-get clean && \
    conda clean --all && \
    mkdir -p /conda-bld/ /.conda/ /.conda_build_locks && \
    chmod 777 /conda-bld/ /.conda/ /.conda_build_locks && \
    chmod o+w /miniconda3/pkgs/

ENV CONDA_PREFIX=/app/
WORKDIR /app
