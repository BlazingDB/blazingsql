#!/bin/bash

hdfs dfs -mkdir -p $(cat /conf_dir/data-swap/dir.info.txt)
