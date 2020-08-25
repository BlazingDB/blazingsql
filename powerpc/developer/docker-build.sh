#bin/bash

cp ../requirements.txt requirements.txt
docker build -t blazingdb/build:powerpc .
rm -rf requirements.txt
