#!/bin/bash

conda build -c blazingsql/label/main -c blazingsql${NIGHTLY} -c rapidsai${NIGHTLY} -c conda-forge -c defaults --python=$PYTHON conda/recipes/blazingsql-dev/
