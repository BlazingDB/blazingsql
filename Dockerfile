ARG CUDA_VER="10.2"
ARG UBUNTU_VERSION="16.04"
FROM nvidia/cuda:${CUDA_VER}-runtime-ubuntu${UBUNTU_VERSION}
LABEL Description="blazingdb/blazingsql is the official BlazingDB environment for BlazingSQL on NIVIDA RAPIDS." Vendor="BlazingSQL" Version="0.4.0"

ARG CUDA_VER=10.2
ARG CONDA_CH="-c blazingsql -c rapidsai -c nvidia"
ARG PYTHON_VERSION="3.7"
ARG RAPIDS_VERSION="0.18"

SHELL ["/bin/bash", "-c"]

RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/tmp \
    --mount=type=cache,target=/usr/local/pkgs \
    apt-get update -qq && \
    apt-get install curl git -yqq --no-install-recommends && \
    curl -s -o /tmp/miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -bfp /usr/local/ && \
    conda create python=${PYTHON_VERSION} -y -n bsql && \
    conda install -y -n bsql \
    ${CONDA_CH} \
    -c conda-forge -c defaults \
    cugraph=${RAPIDS_VERSION} cuml=${RAPIDS_VERSION} \
    cusignal=${RAPIDS_VERSION} \
    cuspatial=${RAPIDS_VERSION} \
    cuxfilter clx=${RAPIDS_VERSION} \
    python=${PYTHON_VERSION} cudatoolkit=${CUDA_VER} \
    blazingsql=${RAPIDS_VERSION} \
    jupyterlab \
    networkx statsmodels xgboost scikit-learn \
    geoviews seaborn matplotlib holoviews colorcet && \
    rm -rf /var/lib/apt/* /tmp/miniconda.sh /usr/local/pkgs/* && \
# Clean in a separate layer as calling conda still generates some __pycache__ files
#  rm -rf /usr/local/envs/bsql/lib/python3.7/site-packages/pip /usr/local/envs/bsql/lib/python3.7/idlelib /usr/local/envs/bsql/lib/python3.7/ensurepip \
#RUN find /usr/local/envs/bsql -name '*.a' -delete && \
  rm -rf /usr/local/envs/bsql/conda-meta && \
    rm -rf /usr/local/envs/bsql/include && \
    rm /usr/local/envs/bsql/lib/libpython3.7m.so.1.0 && \
    find /usr/local/envs/bsql -name '__pycache__' -type d -exec rm -rf '{}' '+' && \
    rm -rf /usr/local/envs/bsql/lib/libasan.so.5.0.0 \
    /usr/local/envs/bsql/lib/libtsan.so.0.0.0 \
    /usr/local/envs/bsql/lib/liblsan.so.0.0.0 \
    /usr/local/envs/bsql/lib/libubsan.so.1.0.0 \
    /usr/local/envs/bsql/bin/x86_64-conda-linux-gnu-ld \
    /usr/local/envs/bsql/bin/sqlite3 \
    /usr/local/envs/bsql/bin/openssl \
    /usr/local/envs/bsql/share/terminfo \
    /usr/local/envs/bsql/bin/postgres \
    /usr/local/envs/bsql/bin/pg_* \
    /usr/local/envs/bsql/man \
    /usr/local/envs/bsql/qml \
    /usr/local/envs/bsql/qsci \
    /usr/local/envs/bsql/mkspecs && \
  find /usr/local/envs/bsql/lib/python3.7/site-packages -name 'tests' -type d -exec rm -rf '{}' '+' && \
  find /usr/local/envs/bsql/lib/python3.7/site-packages -name '*.pyx' -delete && \
  find /usr/local/envs/bsql -name '*.c' -delete && \
  git clone --depth=2 --branch=master https://github.com/BlazingDB/Welcome_to_BlazingSQL_Notebooks /blazingsql && \
  rm -rf /blazingsql/.git && \
#RUN mkdir /pkg && conda run -p /usr/local/envs/bsql python -m pip install --no-deps /pkg
  mkdir /.local /.jupyter /.cupy && chmod 777 /.local /.jupyter /.cupy
#ENV PATH="/bsql/bin:${PATH}" LD_LIBRARY_PATH="/bsql/lib:${LD_LIBRARY_PATH}" CONDA_PREFIX="/bsql/" NUMBAPRO_NVVM="/bsql/lib/libnvvm.so" CUPY_CACHE_DIR="/tmp"
COPY run_jupyter.sh /

# Jupyter
EXPOSE 8888
#CMD ["jupyter-lab" , "--notebook=/blazingsql/", "--allow-root", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token='rapids'"]
CMD ["/run_jupyter.sh"]

