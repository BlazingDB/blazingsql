export DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT="100s"
export DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP="600s"
export DASK_DISTRIBUTED__COMM__RETRY__DELAY__MIN="1s"
export DASK_DISTRIBUTED__COMM__RETRY__DELAY__MAX="60s"
export DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING=True

export DASK_DISTRIBUTED__WORKER__MEMORY__Terminate="False"
export DEVICE_MEMORY_LIMIT="25GB"
export MAX_SYSTEM_MEMORY=$(free -m | awk '/^Mem:/{print $2}')M

ARG_INTERFACE=wlo1
ARG_HOSTNAME=ucx://10.0.0.23:8786

while getopts 'i:h:' o; do
  case "${o}" in
    h)
      ARG_HOSTNAME=${OPTARG}
      ;;
    i)
      ARG_INTERFACE=${OPTARG}
      ;;
  esac
done

# Dask-cuda-worker

UCXPY_NON_BLOCKING_MODE=True \
CUDA_VISIBLE_DEVICES=0  \
DASK_UCX__CUDA_COPY=True \
DASK_UCX__TCP=True \
DASK_UCX__NVLINK=False \
DASK_UCX__INFINIBAND=False \
DASK_UCX__RDMACM=False \
DASK_UCX__REUSE_ENDPOINTS=False \
dask-cuda-worker $ARG_HOSTNAME \
    --interface $ARG_INTERFACE \
    --enable-tcp-over-ucx --device-memory-limit "4GB" --nthreads=8
