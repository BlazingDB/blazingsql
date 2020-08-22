## Readme

### Docker
Building docker image for build:
```
cd blazingsql/powerpc
./docker-build.sh
```

Run a container
```
cd blazingsql/powerpc
docker run -ti --rm blazingdb/build:powerpc bash
```

Run a container with volume
```
cd blazingsql/powerpc
docker run -ti -v $PWD:/app --rm blazingdb/build:powerpc bash
```

Run a container with gpu
```
cd blazingsql/powerpc
docker run -ti -v $PWD:/app --gpus=all --rm blazingdb/build:powerpc bash
```

Run a container with gpu and same user
```
cd blazingsql/powerpc
docker run -u $(id -u):$(id -g) -ti -v /etc/passwd:/etc/passwd -v $PWD:/app --gpus=all --rm blazingdb/build:powerpc bash
```

Execute a command as root:
```
docker exec -u 0:0 -ti <container_id> bash
```

The docker has a pip env in /opt/blazingsql-powerpc-prefix with all the requirements.txt installed

### Python Virtualenv

#### Using the docker pyenv

Activate:
```
source /opt/blazingsql-powerpc-prefix/bin/activate
pip list
```

#### Create new pyenvs

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

#### Run python scripts
For now we need to run python as:
```shell
JAVA_HOME=/usr/lib/jvm/jre CONDA_PREFIX=/opt/blazingsql-powerpc-prefix/ LD_LIBRARY_PATH=/opt/blazingsql-powerpc-prefix/lib:/usr/local/lib64/:/usr/local/lib python
```
