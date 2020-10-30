export DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT="100s"
export DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP="600s"
export DASK_DISTRIBUTED__COMM__RETRY__DELAY__MIN="1s"
export DASK_DISTRIBUTED__COMM__RETRY__DELAY__MAX="60s"
export DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING=True

ARG_INTERFACE=wlo1

export UCX_TLS=tcp,sockcm,cuda_copy,cuda_ipc
export UCX_SOCKADDR_TLS_PRIORITY=sockcm
export UCX_NET_DEVICES=$ARG_INTERFACE
export UCX_MEMTYPE_CACHE=n

while getopts 'i:' o; do
  case "${o}" in
    i)
      ARG_INTERFACE=${OPTARG}
      ;;
  esac
done

#Scheduler
UCXPY_NON_BLOCKING_MODE=True \
DASK_UCX__CUDA_COPY=True \
DASK_UCX__TCP=True \
DASK_UCX__NVLINK=False \
DASK_UCX__INFINIBAND=False \
DASK_UCX__RDMACM=False \
DASK_UCX__REUSE_ENDPOINTS=False \
# DASK_RMM__POOL_SIZE=1GB \
dask-scheduler \
    --scheduler-file dask-scheduler.json \
    --protocol ucx --interface $ARG_INTERFACE
