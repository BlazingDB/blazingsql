#!/bin/bash
set -e

echo "CMD: conda build conda/recipes/blazingsql --python=$PYTHON"
conda build conda/recipes/blazingsql --python=$PYTHON

