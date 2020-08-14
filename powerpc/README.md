## Readme

### Building docker image for build:
```
./docker-build.sh
```

### Run a container
```
docker run -ti -v $PWD:/app blazingdb/build:powerpc bash
```

### Run a container with gpu
```
docker run -ti -v $PWD:/app --gpus=all blazingdb/build:powerpc bash
```

### Dockerfile
nvidia-docker doesn't have powerpc support so this image is only for debugging
