#!/bin/bash

hadoop fs -copyFromLocal /conf_dir/data-swap/* hdfs://$(cat /conf_dir/data-swap/dir.info.txt)
