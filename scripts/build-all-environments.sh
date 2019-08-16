#!/bin/bash

#usage is
# ./build-all-environments.sh 1 1
#  ./build-all-environments.sh [build conda environment] [run conda build]
python_envs=(3.6 3.7)
toolkit_versions=(9.2 10.0)
if [ $# -eq 0 ]
  then
    build_conda_env=1
    run_conda_build=1
  elif [ $# -eq 1 ]
    build_conda_env=$1
    run_conda_build=1
  elif [ $# -eq 2 ]
    build_conda_env=$1
    run_conda_build=$2
fi

for python in "${python_envs[@]}"

do
  for toolkit in "${toolkit_versions[@]}"
  do
    if [ $build_conda_env -eq 1 ]; then
      conda create -n conda-py${python}_cuda${toolkit}
    fi
    conda activate conda-py${python}_cuda${toolkit}
    ./build-all-conda.sh python toolkit &
  done
done

wait

echo "Build Process Finished!"
