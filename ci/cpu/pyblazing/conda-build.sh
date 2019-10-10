#!/bin/bash

# Nightly seccion
echo "IS_NIGHTLY" $IS_NIGHTLY
if [ $IS_NIGHTLY == "true" ]; then
      NIGHTLY="-nightly"
     # CUDF="cudf=0.10"

      libcudf="libcudf=0.10"
      nvstrings="nvstrings=0.10"
      librmm="librmm=0.10"
      daskcudf="dask-cudf=0.10"
      #Replazing cudf version

      echo "Replacing cudf version into meta.yaml"
      sed -ie "s/libcudf/$libcudf/g" conda/recipes/pyBlazing/meta.yaml
      sed -ie "s/nvstrings/$nvstrings/g" conda/recipes/pyBlazing/meta.yaml
      sed -ie "s/librmm/$librmm/g" conda/recipes/pyBlazing/meta.yaml
      sed -ie "s/dask-cudf/$daskcudf/g" conda/recipes/pyBlazing/meta.yaml
fi

conda build -c blazingsql${NIGHTLY} -c rapidsai${NIGHTLY} -c conda-forge -c defaults --python=$PYTHON conda/recipes/pyBlazing/

