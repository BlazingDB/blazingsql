#!/bin/bash

# Nightly seccion
echo "IS_NIGHTLY" $IS_NIGHTLY
if [ $IS_NIGHTLY == "true" ]; then
      NIGHTLY="-nightly"
fi

conda build -c editha$NIGHTLY -c rapidsai$NIGHTLY -c conda-forge -c defaults --python=$PYTHON conda/recipes/pyBlazing/

