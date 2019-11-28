#!/bin/bash

if [ -z ${1+x} ]
then
   INSTALL_PREFIX=$CONDA_PREFIX
else
   INSTALL_PREFIX=$1
fi

mvn clean install -Dmaven.test.skip=true -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/


cp blazingdb-calcite-application/target/BlazingCalcite.jar $INSTALL_PREFIX/lib/blazingsql-algebra.jar
cp blazingdb-calcite-core/target/blazingdb-calcite-core.jar $INSTALL_PREFIX/lib/blazingsql-algebra-core.jar
