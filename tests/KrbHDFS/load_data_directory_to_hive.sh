#!/bin/bash

docker exec krbhdfs_hadoop-secure_1 /root/create_hive_warehouse.sh

echo "Done! Warehouse was created."
