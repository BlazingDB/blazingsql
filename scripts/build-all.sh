#!/bin/bash

cd $CONDA_PREFIX
#set branch to latest to just build develop
repos=(blazingdb-protocol blazingdb-communication blazingdb-io blazingdb-orchestrator blazingdb-ral blazingdb-calcite)
branches=(feature/conda feature/conda feature/conda feature/conda feature/conda latest)
#assumes that you have installed blazingsql-dev into the current conda Environment

i=0
for repo in "${repos[@]}"
do
  cd $CONDA_PREFIX
if [ ! -d "$repo" ]; then
  git clone https://github.com/BlazingDB/$repo
fi
cd $repo
if [ ${branches[i]} != "latest" ]; then
  git checkout ${branches[i]}
fi
i=$(($i+1))

conda/recipes/$repo/build.sh
echo "Cloned and built ${repo}"

done
