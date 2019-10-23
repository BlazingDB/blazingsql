#!/bin/bash

#make sure you are in a conda environment that has
#all of hte blazing dependencies installed

num_gpus="$(nvidia-smi -L | wc -l)"
echo "Found" $num_gpus "GPUS"

mkdir -p log
#by default launches on port 8786
dask-scheduler >log/scheduler.log 2>&1 &

#launch the workers manually per gpu
cd $CONDA_PREFIX
for i in `seq 0 $((num_gpus - 1))`; do
  echo -e "Launching Worker with command: \n bin/blazingsql-engine $i $i 127.0.0.1 9100 127.0.0.1 900$((i+1)) 889$((i+1)) lo >engine.log 2>&1 &"
  bin/blazingsql-engine $i $i 127.0.0.1 9100 127.0.0.1 900$((i+1)) 889$((i+1)) lo >log/engine-$i.log 2>&1 &
  CUDA_VISIBLE_DEVICES=$i dask-worker 127.0.0.1:8786 --worker-port $((8890 + i+101)) --resources "GPU=1" >log/worker-$i.log 2>&1 &
  #you will notice that the dask worker port is exactly 100 more than the engine port
done
