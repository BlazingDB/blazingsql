export DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT="100s"
export DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP="600s"
export DASK_DISTRIBUTED__COMM__RETRY__DELAY__MIN="1s"
export DASK_DISTRIBUTED__COMM__RETRY__DELAY__MAX="60s"
export DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING=True

ARG_INTERFACE=wlo1

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
