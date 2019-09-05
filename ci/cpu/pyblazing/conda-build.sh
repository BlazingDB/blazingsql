#!/bin/bash

conda build -c conda-forge -c defaults -c rapidsai -c editha --python=$PYTHON conda/recipes/pyBlazing/
