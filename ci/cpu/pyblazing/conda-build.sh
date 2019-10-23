#!/bin/bash

conda build -c blazingsql${NIGHTLY} -c rapidsai${NIGHTLY} -c conda-forge -c defaults --python=$PYTHON conda/recipes/pyBlazing/

