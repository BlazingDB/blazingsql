#!/bin/bash

# Wait until the Docker container is ready and running
until [ "`docker-compose logs --no-color --tail=1 | grep ready_krbhdfs_hadoop_secure`" ]; do
    sleep 0.1;
    echo "Starting KrbHDFS services for BlazingSQL testing"
done;

echo "KrbHDFS services ready! You can use this Kerberized HDFS 172.22.0.3:9000"
