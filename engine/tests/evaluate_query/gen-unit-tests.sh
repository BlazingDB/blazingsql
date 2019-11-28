#!/bin/sh
CWD="$(pwd)"
echo "Build cpp library"
cd ../../resources/unit_test-generator

#example: ~/blazingdb/blazingdb-calcite/blazingdb-calcite-cli/target/BlazingCalciteCli.jar
CalciteCli=$1
python3 generator.py queries.json $CalciteCli -O ../../tests/evaluate_query/evaluate_query.cu
cd $CWD
clang-format -i -style=Google evaluate_query.cu
