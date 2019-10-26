#!/bin/bash

conda build -c editha${NIGHTLY}/label/main/ -c rapidsai${NIGHTLY} -c conda-forge -c defaults --python=$PYTHON conda/recipes/pyBlazing/

