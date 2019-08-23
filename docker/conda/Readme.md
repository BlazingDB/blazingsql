
Generate Docker image:
```
$ docker build -t blazingdb/blazingsql:condav1 docker/conda
```

Generate Docker image from Ubuntu 18.04 and Cuda 10.0:
```
$ docker build -t blazingdb/conda:ubuntu1804-cuda10-v1 --build-arg UBUNTU_VERSION=18.04 --build-arg CUDA_VERSION=10.0 docker/conda/
```

Run a container
```
$ docker run -ti -v $PWD/:/app blazingdb/conda:ubuntu1804-cuda10-v1 bash
```

Generate all conda packages:
```
# cd /root && /app/scripts/build-all-conda.sh 3.7 cuda10.0 blazingdb
```

Upload to Anaconda cloud
```
# anaconda upload --user BlazingDB /blazing-build/py3.7_cudacuda10.0/linux-64/blazingsql-python-0.4.0-py37hf484d3e_0.tar.bz2 --label cuda10.0 --label cuda9.2 --label main
```
