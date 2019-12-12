#!/bin/bash
# usage: ./build.sh prefix run_test
# example: ./build.sh $CONDA_PREFIX ON|OFF

INSTALL_PREFIX=$CONDA_PREFIX
if [ ! -z $1 ]; then
  INSTALL_PREFIX=$1
fi

run_test="false"
if [ ! -z $2 ]; then
  run_test="true"
fi

echo "CMD: mvn clean install -Dmaven.test.skip=$run_test -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/"
mvn clean install -Dmaven.test.skip=$run_test -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/


cp blazingdb-calcite-application/target/BlazingCalcite.jar $INSTALL_PREFIX/lib/blazingsql-algebra.jar
cp blazingdb-calcite-core/target/blazingdb-calcite-core.jar $INSTALL_PREFIX/lib/blazingsql-algebra-core.jar
