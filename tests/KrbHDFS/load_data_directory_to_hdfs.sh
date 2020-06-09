#!/bin/bash

# USAGE: load_data_directory_to_hdfs.sh data-dir

data_dir=$1

echo "Data folder: $data_dir"

mkdir -p myconf/data-swap
cp -r $data_dir/* myconf/data-swap
echo $data_dir > myconf/data-swap/dir.info.txt

docker exec krbhdfs_hadoop-secure_1 /root/create_dir_from_info.sh
docker exec krbhdfs_hadoop-secure_1 /root/load_swap_dir.sh

echo "Done! Data is loaded in hdfs:/$data_dir"
