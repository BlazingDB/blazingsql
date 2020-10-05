#!/bin/bash

HADOOP_DIR=$1
DATA_DIR=$2

echo "HADOOP_DIR = "$HADOOP_DIR

sudo cp /etc/hosts /etc/hosts.old

echo "172.22.0.2 kdc.kerberos.com" | sudo tee -a  /etc/hosts
echo "172.22.0.3 hadoop.docker.com" | sudo tee -a  /etc/hosts

echo $PWD

cd ../KrbHDFS

echo $PWD

echo "Copying files to Hadoop Dir"
cp hdfs/config_files/core-site.xml $HADOOP_DIR/etc/hadoop/
cp hdfs/config_files/hdfs-site.xml $HADOOP_DIR/etc/hadoop/

#echo "Setting env vars"
#source load_hdfs_env_vars.sh $HADOOP_DIR

if [ -x "$(command -v docker)" ]; then

    if [ -x "$(command -v docker-compose)" ]; then
        echo "Docker initilization.."
        echo $PWD
        HOSTNAME=$HOSTNAME docker-compose up -d

        echo "Sleeping.."
        sleep 30

        echo "Checking.."
#        ./check_is_online.sh
        ./load_data_directory_to_hdfs.sh $DATA_DIR/tpch
        ./load_data_directory_to_hive.sh
    else
        echo "docker-compose is required!"
        exit 1
    fi
else
    echo "Docker is required!"
    exit 1
fi

cd myconf
sudo ./conf-my-host.sh
cd ..
