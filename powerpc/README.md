## Readme

### Docker
Building docker image for build:
```
./docker-build.sh
```

Run a container
```
docker run -ti --rm blazingdb/build:powerpc bash
```

Run a container with volume
```
docker run -ti -v $PWD:/app --rm blazingdb/build:powerpc bash
```

Run a container with gpu
```
docker run -ti -v $PWD:/app --gpus=all --rm blazingdb/build:powerpc bash
```

Run a container with gpu and same user
```
docker run -u $(id -u):$(id -g) -ti -v /etc/passwd:/etc/passwd -v $PWD:/app --gpus=all --rm blazingdb/build:powerpc bash
```

Execute a command as root:
```
docker exec -u 0:0 -ti <container_id> bash
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

Install Python dependencies:
```
pip install -r requirements.txt
```

Deactivate:
```
deactivate
```

Dockerfile
nvidia-docker doesn't have powerpc support so this image is only for debugging
