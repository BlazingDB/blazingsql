#!/bin/bash
# usage:   ./conda-upload-docker.sh user pass cuda_version path_file
# example: ./conda-upload-docker.sh myuser clave 10.0|9.2 py3.6_cuda9.2/linux-64/blazingsql-protocol-0.4.0-cuda9.2_py36_123.tar.bz2

docker run --rm -v $PWD/:/app/ blazingdb/blazingsql:conda-cuda$3 /app/conda-upload.sh $1 $2 $3 $4
