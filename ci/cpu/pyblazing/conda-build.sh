#!/bin/bash

conda build -c conda-forge -c defaults -c rapidsai -c blazingsql --python=$PYTHON conda/recipes/blazingdb-pyblazing/
