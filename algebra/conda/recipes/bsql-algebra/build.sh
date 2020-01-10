#!/bin/bash
# usage: ./build.sh run_test prefix
# example: ./build.sh ON|OFF $CONDA_PREFIX

run_test="ON"
if [ ! -z $1 ]; then
  run_test=$1
fi

INSTALL_PREFIX=$CONDA_PREFIX
if [ ! -z $2 ]; then
  INSTALL_PREFIX=$2
fi

if [ "$run_test" == "ON" ]; then
  echo "CMD: mvn clean install -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/"
  mvn clean install -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/
else
  echo "CMD: mvn clean install -Dmaven.test.skip=true -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/"
  mvn clean install -Dmaven.test.skip=true -f pom.xml -Dmaven.repo.local=$INSTALL_PREFIX/blazing-protocol-mvn/
fi
if [ $? != 0 ]; then
  exit 1
fi

cp blazingdb-calcite-application/target/BlazingCalcite.jar $INSTALL_PREFIX/lib/blazingsql-algebra.jar
cp blazingdb-calcite-core/target/blazingdb-calcite-core.jar $INSTALL_PREFIX/lib/blazingsql-algebra-core.jar
