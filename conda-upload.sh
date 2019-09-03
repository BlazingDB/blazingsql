#!/bin/bash
# usage: ./conda-upload.sh user pass label path_file

anaconda login --username $1 --password $2
anaconda upload --user blazingsql --label cuda$3 $4
