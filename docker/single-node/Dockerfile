ARG CUDA_VERSION=9.2
FROM nvidia/cuda:${CUDA_VERSION}-devel-ubuntu16.04

MAINTAINER BlazingSQL <felipe@blazingsql.com>

LABEL Description="blazingsql/blazingsql is the official BlazingDB environment for BlazingSQL on NIVIDA RAPIDS." Vendor="BlazingSQL" Version="0.4.0"

RUN apt-get update
RUN apt-get install -y gcc make curl


# Install miniconda to /miniconda
RUN curl -LO http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh
RUN bash Miniconda-latest-Linux-x86_64.sh -p /miniconda -b
RUN rm Miniconda-latest-Linux-x86_64.sh
ENV PATH=/miniconda/bin:${PATH}
RUN conda update -y conda

# Setup the Calcite database catalog
RUN mkdir /blazingsql


RUN conda install -y \
      -c rapidsai-nightly/label/xgboost -c rapidsai-nightly -c nvidia -c conda-forge \
      python=3.6 cudatoolkit=${CUDA_VERSION} \
      cudf=0.10.* cuml=0.10.* cugraph=0.10.* gcsfs pynvml pip jupyter \
      dask-cudf=0.10.* \
      rapidsai/label/xgboost::xgboost=>0.9

RUN conda install -y -c rapidsai-nightly -c nvidia -c conda-forge -c felipeblazing/label/cuda10.0 python=3.6 cudatoolkit=10.0 blazingsql-ral blazingsql-orchestrator blazingsql-calcite blazingsql-python
RUN pip install flatbuffers




USER root


# NOTE Fix some numba issues (NvvmSupportError: libNVVM cannot be found)
#ENV NUMBAPRO_NVVM /usr/local/cuda-9.2/nvvm/lib64/libnvvm.so
#ENV NUMBAPRO_LIBDEVICE /usr/local/cuda-9.2/nvvm/libdevice/


# Copy the demo files
#RUN mkdir -p /blazingdb/data/
#RUN mkdir -p /blazingdb/notebooks/
#COPY data /blazingdb/data/
#COPY notebooks/ /blazingdb/notebooks/
#RUN chown -R jupyter:jupyter /blazingdb/

# NOTE Expose CUDF runtime
ENV LD_LIBRARY_PATH /miniconda/lib/

# If the user wants to persist the blazingsql catalog (schema store)
VOLUME /blazingsql

# If the user wants to communicate by unix socket need to mount his /tmp folder
VOLUME /tmp

# Jupyter
EXPOSE 80

CMD ["jupyter", "notebook", "--port", "80"]
