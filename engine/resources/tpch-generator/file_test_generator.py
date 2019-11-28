#!/usr/bin/env python

import argparse
import json
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description='Generate RAL test data.')
    parser.add_argument('filename', type=str,
                        help='Fixture JSON file name')
    parser.add_argument('calcite_jar', type=str,
                        help='Calcite CLI Jar')
    parser.add_argument('-O', '--output', type=str, default='-',
                        help='Output file path or - for stdout')
    args = parser.parse_args()

    items = make_items(args.filename)

    plans = make_plans(items, args.calcite_jar)

    strings_classes = (make_unit_test(item, plan)
                       for item, plan in zip(items, plans))
    header_text = '\n'.join(strings_classes)

    write(header_text).to(args.output)


def make_items(filename):
    with sys.stdin if '-' == filename else open(filename) as jsonfile:
        return [item_from(dct) for dct in json.load(jsonfile)]


def make_plans(items, calcite_jar):
    def inputjson(item):
        json_obj = json.dumps({'query': item.query, 'tables': item.tables})
        print(json_obj)
        return re.findall('non optimized\\n(.*)\\n\\noptimized',
                          subprocess.Popen(('java', '-jar', calcite_jar),
                                           stdin=subprocess.PIPE,
                                           stdout=subprocess.PIPE).communicate(json_obj.encode())[0].decode('utf-8'), re.M | re.S)[0]

    return [inputjson(item) for item in items]


def item_from(dct):
    return type('json_object', (), {
        key: item_from(value) if type(value) is dict else value
        for key, value in dct.items()})


def make_unit_test(item, plan):
    return ('TEST_F(EvaluateQueryTest, %(test_name)s) {'
            'auto input = %(input)s;'
            'auto logical_plan = input.logicalPlan;'
            'auto input_tables = ToBlazingFrame(input.filePaths, input.columnNames, input.columnTypes);'
            'auto table_names = input.tableNames;'
            'auto column_names = input.columnNames;'
            'std::vector<gdf_column_cpp> outputs;'
            'gdf_error err = evaluate_query(input_tables, table_names, column_names, logical_plan, outputs);'
            'EXPECT_TRUE(err == GDF_SUCCESS);'
            'auto output_table = GdfColumnCppsTableBuilder{"output_table", outputs}.Build();'
            'CHECK_RESULT(output_table, input.resultTable);'
            '}') % {'test_name': item.testName, 'input': Φ(item, plan)}


def get_file_paths(tables):
    items = ['"%s"' % table['filePath'] for table in tables]
    return '{%s}' % (','.join(items))


def get_table_names(tables):
    items = ['"main.%s"' % table['tableName'] for table in tables]
    return '{%s}' % (','.join(items))


def get_column_names(tables):
    def get_list(columnNames):
        return '{%s}' % (','.join(['"%s"' % name for name in columnNames]))

    items = [get_list(table['columnNames']) for table in tables]
    return '{%s}' % (','.join(items))


def get_column_types(tables):
    def native_type(val):
        return {
            'GDF_FLOAT64': 'float64',
            'GDF_FLOAT32': 'float32',
            'GDF_INT8': 'int',
            'GDF_INT32': 'int32',
            'GDF_INT64': 'int64',
        }[val]

    def get_list(columnTypes):
        return '{%s}' % (','.join(['"%s"' % native_type(val) for val in columnTypes]))

    items = [get_list(table['columnTypes']) for table in tables]
    return '{%s}' % (','.join(items))


def Φ(item, plan):
    return (
        '''InputTestItem{
        .query = "%(query)s",
        .logicalPlan ="%(plan)s",
        .filePaths = %(filePaths)s,
        .tableNames = %(tableNames)s,
        .columnNames = %(columnNames)s,
        .columnTypes = %(columnTypes)s,
        .resultTable = %(resultTable)s}
  ''') % {
        'query': item.query,
        'plan': '\\n'.join(line for line in plan.split('\n')),
        'filePaths': get_file_paths(item.tables),
        'tableNames': get_table_names(item.tables),
        'columnNames': get_column_names(item.tables),
        'columnTypes': get_column_types(item.tables),
        'resultTable': make_table(item.result, 'ResultSet', item.resultTypes, item.resultTypes),
    }


def make_table(data, tableName, columnNames, columnTypes):
    return ('LiteralTableBuilder{"%(tableName)s",'
            ' %(literals)s}.Build()') % {
        'tableName': tableName,
        'literals': make_literals(data, columnNames, columnTypes),
    }


def make_table_group(data, tableName, columnNames, columnTypes):
    return ('LiteralTableGroupBuilder{{"%(tableName)s",'
            ' %(literals)s}}.Build()') % {
        'tableName': tableName,
        'literals': make_literals(data, columnNames, columnTypes),
    }


def make_literals(data, columnNames, columnTypes):
    return '{%s}' % (
        ','.join(['{"%s", Literals<%s>{%s} }'
                  % (name, _type, ','.join(str(x) for x in values))
                  for name, _type, values in zip(columnNames, columnTypes, data)]))


def write(header_text):
    def to(filename):
        with sys.stdout if '-' == filename else open(filename, 'w') as output:
            output.write(HEADER_DEFINITIONS % header_text)
    return type('writer', (), dict(to=to))


HEADER_DEFINITIONS = '''
# include <cstdlib>
# include <iostream>
# include <vector>
# include <string>

# include <gtest/gtest.h>
# include <CalciteExpressionParsing.h>
# include <CalciteInterpreter.h>
# include <StringUtil.h>
# include <DataFrame.h>
# include <GDFColumn.cuh>
# include <GDFCounter.cuh>
# include <Utils.cuh>

# include "gdf/library/api.h"
using namespace gdf::library;

#include <sys/stat.h>
#include <gdf/cffi/functions.h>
#include <gdf/gdf.h>

#include "csv_utils.cuh"

using namespace gdf::library;


struct EvaluateQueryTest : public ::testing::Test {
  struct InputTestItem {
    std::string query;
    std::string logicalPlan;
    std::vector<std::string> filePaths;
    std::vector<std::string> tableNames;
    std::vector<std::vector<std::string>> columnNames;
    std::vector<std::vector<const char*>> columnTypes;
    gdf::library::Table resultTable;
  };

  void CHECK_RESULT(gdf::library::Table& computed_solution,
                    gdf::library::Table& reference_solution) {
    computed_solution.print(std::cout);
    reference_solution.print(std::cout);

    for (size_t index = 0; index < reference_solution.size(); index++) {
      const auto& reference_column = reference_solution[index];
      const auto& computed_column = computed_solution[index];
      auto a = reference_column.to_string();
      auto b = computed_column.to_string();
      EXPECT_EQ(a, b);
    }
  }
};

// AUTO GENERATED UNIT TESTS
%s

'''


if '__main__' == __name__:
    main()
