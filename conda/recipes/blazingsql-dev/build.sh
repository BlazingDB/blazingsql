#!/bin/bash


INSTALL_PREFIX=${INSTALL_PREFIX:=${PREFIX:=${CONDA_PREFIX}}}
cd ${INSTALL_PREFIX}

echo -e "
#!/bin/bash
cd $CONDA_PREFIX
git clone https://github.com/BlazingDB/pyBlazing.git
pyBlazing/scripts/build-all.sh
" > build-repos.sh

chmod +x build-repos.sh 
