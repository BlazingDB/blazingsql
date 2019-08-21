#!/bin/bash
python=$1
toolkit=$2
channel=$3
if [ -z "$4" ]
  then
    label="main"
  else
    label=$4
fi


#usage is
# ./build-all-conda.sh 3.6 9.2 felipeblazing cuda10.0
#  ./build-all-conda.sh [python version] [cuda toolkit version] [channel] [label (optional)]


echo "$CONDA_PREFIX: "$CONDA_PREFIX
cd $CONDA_PREFIX
#set branch to latest to just build develop
repos=(blazingdb-protocol blazingdb-communication blazingdb-io blazingdb-orchestrator blazingdb-ral blazingdb-calcite)
branches=(feature/conda feature/conda feature/conda feature/conda feature/conda latest)
#assumes that you have installed blazingsql-dev into the current conda Environment
mkdir $CONDA_PREFIX/blazing-build/py${python}_cuda${toolkit} -p
i=0
for repo in "${repos[@]}"
do
  echo "######################################################################### Start ${repo} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
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
    git pull origin ${branches[i]}
  fi
  i=$(($i+1))

  echo "repo_dir: conda/recipes/"$repo
  cd conda/recipes/$repo

  conda build --label label -c conda-forge -c $channel -c rapidsai-nightly --python=$python --output-folder $CONDA_PREFIX/blazing-build/py${python}_cuda${toolkit} .
  if [ $? != 0 ]; then
    #exit 1
    echo "######################################################################### Build failed ${repo} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
  else
    echo "######################################################################### Cloned and built ${repo} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
  fi


done
