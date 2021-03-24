#!/bin/bash

source activate bsql
jupyter-lab --notebook=/blazingsql/ --allow-root --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token=''
