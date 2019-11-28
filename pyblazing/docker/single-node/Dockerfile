ARG CUDA_VERSION=10.0
ARG BLAZING_CHANNEL=felipeblazing/label/cuda10.0

FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu16.04

MAINTAINER BlazingSQL <felipe@blazingsql.com>

LABEL Description="blazingsql/blazingsql is the official BlazingDB environment for BlazingSQL on NIVIDA RAPIDS." Vendor="BlazingSQL" Version="0.4.0"

RUN apt-get update && \
    apt-get install -y gcc make curl wget

# Install miniconda to /miniconda
ENV PATH=${PATH}:/miniconda3/bin
RUN wget -q -O /tmp/miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -bfp /miniconda3/ && \
    rm -f /tmp/miniconda.sh && \
    conda update -y conda && \
    conda clean --all

# Setup the Calcite database catalog
RUN mkdir /blazingsql

RUN conda install -y -c conda-forge -c defaults -c nvidia -c rapidsai -c blazingsql/label/cuda10.0 blazingsql-calcite blazingsql-orchestrator blazingsql-ral blazingsql-python python=3.7 cudatoolkit=10.0
RUN pip install flatbuffers && \
    pip install jupyterlab

# NOTE Expose CUDF runtime
ENV LD_LIBRARY_PATH /miniconda3/lib/

# If the user wants to persist the blazingsql catalog (schema store)
VOLUME /blazingsql

# If the user wants to communicate by unix socket need to mount his /tmp folder
VOLUME /tmp

# Jupyter
EXPOSE 80

CMD ["jupyter-lab", "--notebook=/blazingsql/", "--allow-root", "--ip=0.0.0.0", "--port=80", "--no-browser", "--NotebookApp.token='rapids'"]
