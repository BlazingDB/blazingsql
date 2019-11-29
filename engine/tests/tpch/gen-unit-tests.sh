#!/bin/sh
CWD="$(pwd)"
echo "Build cpp library"
cd ../../resources/tpch-generator

#example:
# ./gen_unit_tests where_queries.json ~/blazingdb/blazingdb-calcite/blazingdb-calcite-cli/target/BlazingCalciteCli.jar tpch-tests.cu

JsonFile=$1
CalciteCli=$2
OutputFile=$3
python3  test_generator.py $JsonFile $CalciteCli -O ../../tests/tpch/$OutputFile
cd $CWD
clang-format -i -style=Google $OutputFile
