
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
$ docker run -ti -v $PWD/:/app blazingdb/blazingsql:condav1 bash
```

Generate all conda packages:
```
# cd /root && /app/scripts/build-all-conda.sh 3.7 cuda9.2 felipeblazing
```
