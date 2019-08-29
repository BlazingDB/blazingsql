#!/bin/bash

cd $CONDA_PREFIX
#set branch to latest to just build develop
repos=(blazingdb-protocol blazingdb-communication blazingdb-io blazingdb-orchestrator blazingdb-ral pyBlazing blazingdb-calcite)
branches=(feature/conda feature/conda feature/conda feature/conda feature/conda feature/conda latest)
build_type=(Release Release Release Release Debug Release Release)

#assumes that you have installed blazingsql-dev into the current conda Environment

i=0
for repo in "${repos[@]}"
do
  cd $CONDA_PREFIX
  if [ ! -d "$repo" ]; then
    git clone https://github.com/BlazingDB/$repo
  else
    cd $repo
    if [ ! -d ".git" ]; then # the folder existed but its not a repo. Lets delete it and actually get the repo
      cd ..
      rm -r $repo
      git clone https://github.com/BlazingDB/$repo
    else
      cd ..
    fi
  fi
  cd $repo
  if [ ${branches[i]} != "latest" ]; then
    git fetch
    git checkout ${branches[i]}
    git pull
  fi
  i=$(($i+1))

  chmod +x conda/recipes/$repo/build.sh
  conda/recipes/$repo/build.sh ${build_type[i]}
  echo "######################################################################### Cloned and built ${repo} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"

done
