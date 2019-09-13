#!/bin/bash
# Usage ./build-all.sh branch

repos=(blazingdb-protocol blazingdb-communication blazingdb-io blazingdb-orchestrator blazingdb-ral pyBlazing blazingdb-calcite)
branch="develop"
if [ ! -z $1 ]; then
  branch=$1
fi

#assumes that you have installed blazingsql-dev into the current conda Environment
i=0
for repo in "${repos[@]}"
do
  cd $CONDA_PREFIX

  echo "### Start $repo ###"
  if [ ! -d "$repo" ]; then
    git clone -b $branch https://github.com/BlazingDB/$repo
  else
    cd $repo
    if [ ! -d ".git" ]; then # the folder existed but its not a repo. Lets delete it and actually get the repo
      cd ..
      rm -r $repo
      git clone -b $branch https://github.com/BlazingDB/$repo
    else
      cd ..
    fi
  fi
  cd $repo && git reset --hard && git checkout $branch && git pull origin $branch
  i=$(($i+1))

  chmod +x conda/recipes/$repo/build.sh
  status="Cloned and built"
  conda/recipes/$repo/build.sh
  if [ $? != 0 ]; then
    status="Build failed"
  fi

  echo "######################################################################### ${status} ${repo} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"

done
