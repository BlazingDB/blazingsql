
Generate Docker image:
```
$ docker build -t blazingdb/blazingsql:condav1 docker/conda
```

Run a container
```
$ docker run -ti -v $PWD/:/app blazingdb/blazingsql:condav1 bash
```

Generate all conda packages:
```
# cd /root && /app/scripts/build-all-conda.sh 3.7 cuda9.2 felipeblazing
```
