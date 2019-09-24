#!/bin/bash
# Usage   ./build-all.sh clean_build
# Example ./build-all.sh true

repos=(blazingdb-protocol blazingdb-communication blazingdb-io blazingdb-orchestrator blazingdb-ral pyBlazing blazingdb-calcite)
branches=(develop develop develop develop develop develop develop)
# Release or DEBUG
types=(Release Release Release Release Release Release Release)


clean_build="false"
if [ ! -z $1 ]; then
  clean_build="true"
fi

#assumes that you have installed blazingsql-dev into the current conda Environment
i=0
for repo in "${repos[@]}"
do
  cd $CONDA_PREFIX

  echo "### Start $repo ###"
  branch=${branches[i]}
  type=${types[i]}
  echo "Branch: "$branch
  echo "Type: "$type
  echo "Clean: "$clean_build

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

  cd $repo

  if [ "$clean_build" == "true" ]; then
    git reset --hard && git checkout $branch && git pull origin $branch
  fi

  chmod +x conda/recipes/$repo/build.sh

  status="Cloned and built"
  failed=0
  conda/recipes/$repo/build.sh $type
  if [ $? != 0 ]; then
    status="Build failed"
    failed=1
  fi

  echo "######################################################################### ${status} ${repo} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
  if [ $failed != 0 ]; then
    exit 1
  fi

  i=$(($i+1))

done
