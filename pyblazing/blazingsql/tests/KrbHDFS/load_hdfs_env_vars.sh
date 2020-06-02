#!/bin/bash

# USAGE: source env_vars.sh /path/to/hadoop_dir

# NOTE example /downloads/hadoop-2.7.4
hadoop_home_dir=$1

if [ -z "$hadoop_home_dir" ]; then
    echo "hadoop home dir argument is not defined, e.g. source env_vars.sh /downloads/hadoop-2.7.4"
    exit 1
fi

# NOTE: Usage 'source '

if [ -z "$CONDA_PREFIX" ]; then
    echo "\$CONDA_PREFIX is not defined, activate your conda env!"
    exit 1
fi

export JAVA_HOME=$CONDA_PREFIX

if [ -z "$JAVA_HOME" ]; then
    echo "\$JAVA_HOME is not defined, install java for your conda env!"
    exit 1
fi

export HADOOP_HOME=$hadoop_home_dir
export PATH=$PATH:$HADOOP_HOME/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native/
export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`
export ARROW_LIBHDFS_DIR=$PATH:$HADOOP_HOME/lib/native/
