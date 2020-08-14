## Readme

### Building docker image for build:
```
./docker-build.sh
```

### Run a container
```
docker run -ti --rm blazingdb/build:powerpc bash
```

### Run a container with volume
```
docker run -ti -v $PWD:/app --rm blazingdb/build:powerpc bash
```

### Run a container with gpu
```
docker run -ti -v $PWD:/app --gpus=all --rm blazingdb/build:powerpc bash
```

### Run a container with gpu and same user
```
docker run -u $(id -u):$(id -g) -ti -v $PWD:/app --gpus=all --rm blazingdb/build:powerpc bash
```

### Python Virtualenv
Create:
```
python3 -m venv demo
```

Activate:
```
source demo/bin/activate
pip list
```

Deactivate:
```
deactivate
```

### Dockerfile
nvidia-docker doesn't have powerpc support so this image is only for debugging
