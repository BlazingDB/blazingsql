ARG CUDA_VER="10.2"
ARG UBUNTU_VERSION="16.04"
FROM gpuci/miniforge-cuda:${CUDA_VER}-runtime-ubuntu${UBUNTU_VERSION} as conda

ARG CUDA_VER=10.2
ARG CONDA_CH="-c blazingsql -c rapidsai -c nvidia"
ARG PYTHON_VERSION="3.7"
ARG RAPIDS_VERSION="0.18"

RUN --mount=type=cache,target=/opt/conda/pkgs conda create python=${PYTHON_VERSION} -y -p /bsql && \
    conda install -y -p /bsql \
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
    geoviews seaborn matplotlib holoviews colorcet
# Clean in a separate layer as calling conda still generates some __pycache__ files
#  rm -rf /bsql/lib/python3.7/site-packages/pip /bsql/lib/python3.7/idlelib /bsql/lib/python3.7/ensurepip \
#RUN find /bsql -name '*.a' -delete && \
RUN rm -rf /bsql/conda-meta && \
  rm -rf /bsql/include && \
  rm /bsql/lib/libpython3.7m.so.1.0 && \
  find /bsql -name '__pycache__' -type d -exec rm -rf '{}' '+' && \
  rm -rf /bsql/lib/libasan.so.5.0.0 \
    /bsql/lib/libtsan.so.0.0.0 \
    /bsql/lib/liblsan.so.0.0.0 \
    /bsql/lib/libubsan.so.1.0.0 \
    /bsql/bin/x86_64-conda-linux-gnu-ld \
    /bsql/bin/sqlite3 \
    /bsql/bin/openssl \
    /bsql/share/terminfo \
    /bsql/bin/postgres \
    /bsql/bin/pg_* \
    /bsql/man \
    /bsql/qml \
    /bsql/qsci \
    /bsql/mkspecs && \
  find /bsql/lib/python3.7/site-packages -name 'tests' -type d -exec rm -rf '{}' '+' && \
  find /bsql/lib/python3.7/site-packages -name '*.pyx' -delete && \
  find /bsql -name '*.c' -delete
#RUN mkdir /pkg && conda run -p /bsql python -m pip install --no-deps /pkg
RUN git clone --depth=2 --branch=master https://github.com/BlazingDB/Welcome_to_BlazingSQL_Notebooks /blazingsql


ARG CUDA_VER="10.2"
ARG UBUNTU_VERSION="16.04"
FROM nvidia/cuda:${CUDA_VER}-runtime-ubuntu${UBUNTU_VERSION}
#FROM nvidia/cuda:10.2-runtime-ubuntu18.04
LABEL Description="blazingdb/blazingsql is the official BlazingDB environment for BlazingSQL on NIVIDA RAPIDS." Vendor="BlazingSQL" Version="0.4.0"
COPY --from=conda /bsql /bsql
COPY --from=conda /blazingsql /blazingsql
RUN mkdir /.local /.jupyter /.cupy && chmod 777 /.local /.jupyter /.cupy
ENV PATH="/bsql/bin:${PATH}" LD_LIBRARY_PATH="/bsql/lib:${LD_LIBRARY_PATH}" CONDA_PREFIX="/bsql/" NUMBAPRO_NVVM="/bsql/lib/libnvvm.so" CUPY_CACHE_DIR="/tmp"

# Jupyter
EXPOSE 8888
CMD ["jupyter-lab" , "--notebook=/blazingsql/", "--allow-root", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token='rapids'"]

