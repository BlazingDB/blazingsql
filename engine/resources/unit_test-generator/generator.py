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

  strings_classes = (make_unit_test(item, plan) for item, plan in zip(items, plans))

  header_text = '\n'.join(strings_classes)

  write(header_text).to(args.output)


def make_items(filename):
  with sys.stdin if '-' == filename else open(filename) as jsonfile:
    return [item_from(dct) for dct in json.load(jsonfile)]


def make_plans(items, calcite_jar):
  inputjson = lambda item: re.findall('non optimized\\n(.*)\\n\\noptimized',
      subprocess.Popen(('java', '-jar', calcite_jar),
                       stdin=subprocess.PIPE,
                       stdout=subprocess.PIPE).communicate(json.dumps({
                         'query': item.query,
                         'tables': [
                            {
                              'columnNames': item.schema.columnNames,
                              'columnTypes': item.schema.columnTypes,
                              'tableName': item.schema.tableName,
                              'dbName': item.schema.dbName
                            }
                         ]
                       }).encode())[0].decode('utf-8'), re.M|re.S)[0]
  return (inputjson(item) for item in items)


def item_from(dct):
  return type('json_object', () ,{
    key: item_from(value) if type(value) is dict else value
    for key, value in dct.items()})

def make_unit_test(item, plan):
  return ('TEST_F(EvaluateQueryTest, %(test_name)s) {'
    'auto input = %(input)s;'
		'auto logical_plan = input.logicalPlan;'
		'auto input_tables = input.tableGroup.ToBlazingFrame();'
		'auto table_names = input.tableGroup.table_names();'
		'auto column_names = input.tableGroup.column_names();'
		'std::vector<gdf_column_cpp> outputs;'
		'gdf_error err = evaluate_query(input_tables, table_names, column_names, logical_plan, outputs);'
		'EXPECT_TRUE(err == GDF_SUCCESS);'
    'auto output_table = GdfColumnCppsTableBuilder{"output_table", outputs}.Build();'
    'CHECK_RESULT(output_table, input.resultTable);'
        '}') % {'test_name': item.testName, 'input': Φ(item, plan)}




def Φ(item, plan):
  return ('InputTestItem{.query = "%(query)s", .logicalPlan ="%(plan)s",'
          ' .tableGroup = %(tableGroup)s, .resultTable = %(resultTable)s}') % {
  'query': item.query,
  'plan': '\\n'.join(line for line in plan.split('\n')),
  'tableGroup': make_table_group(item.data, item.schema.dbName + '.' + item.schema.tableName, item.schema.columnNames, item.schema.columnTypes),
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
#include <cstdlib>
#include <iostream>
#include <vector>
#include <string>

#include <gtest/gtest.h>
#include <CalciteExpressionParsing.h>
#include <CalciteInterpreter.h>
#include <StringUtil.h>
#include <DataFrame.h>
#include <GDFColumn.cuh>
#include <GDFCounter.cuh>
#include <Utils.cuh>
#include <gdf/gdf.h>

#include "gdf/library/api.h"
using namespace gdf::library;


struct EvaluateQueryTest : public ::testing::Test {

	struct InputTestItem {
		std::string query;
		std::string logicalPlan;
		gdf::library::TableGroup tableGroup;
		gdf::library::Table resultTable;
	};

 	void CHECK_RESULT(gdf::library::Table& computed_solution, gdf::library::Table& reference_solution){
		 computed_solution.print(std::cout);
		 reference_solution.print(std::cout);

		 for (size_t index = 0; index < reference_solution.size(); index++) {
       const auto &reference_column = reference_solution[index];
       const auto &computed_column = computed_solution[index];
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
