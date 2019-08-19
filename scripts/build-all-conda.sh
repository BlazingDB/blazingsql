#!/bin/bash
python=$1
toolkit=$2


#usage is
# ./build-all-environments.sh 3.6 9.2
#  ./build-all-environments.sh [python version] [cuda toolkit version]

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
fi
cd $repo
if [ ${branches[i]} != "latest" ]; then
  git checkout ${branches[i]}
fi
i=$(($i+1))

cd conda/recipes/$repo
conda build -c conda-forge -c felipeblazing -c rapidsai-nightly --python=$python --output-folder $CONDA_PREFIX/blazing-build/py${python}_cuda${toolkit} .
echo "Cloned and conda built ${repo}"

done
