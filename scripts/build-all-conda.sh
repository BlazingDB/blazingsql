#!/bin/bash
python=$1
toolkit=$2
if [ -z "$3" ]
  then
    label="main"
  else
    label=$3
fi



#usage is
# ./build-all-conda.sh 3.6 9.2
#  ./build-all-conda.sh [python version] [cuda toolkit version]

cd $CONDA_PREFIX
#set branch to latest to just build develop
repos=(blazingdb-protocol blazingdb-communication blazingdb-io blazingdb-orchestrator blazingdb-ral blazingdb-calcite)
branches=(feature/conda feature/conda feature/conda feature/conda feature/conda latest)
#assumes that you have installed blazingsql-dev into the current conda Environment
mkdir $CONDA_PREFIX/blazing-build/py${python}_cuda${toolkit} -p
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
    git checkout ${branches[i]}
  fi
  i=$(($i+1))

  cd conda/recipes/$repo
  conda build --label label -c conda-forge -c felipeblazing -c rapidsai-nightly --python=$python --output-folder $CONDA_PREFIX/blazing-build/py${python}_cuda${toolkit} .
  echo "######################################################################### Cloned and built ${repo} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"

done
