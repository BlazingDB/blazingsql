#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "Traits/RuntimeTraits.h"

#include "io/DataLoader.h"
#include "io/data_parser/CSVParser.h"
#include "io/data_parser/DataParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_provider/DataProvider.h"
#include "io/data_provider/UriDataProvider.h"
#include <DataFrame.h>
#include <fstream>
#include <gdf_wrapper/gdf_wrapper.cuh>

#include <GDFColumn.cuh>

#include <GDFColumn.cuh>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>

using blazingdb::manager::Context;
using Node = blazingdb::transport::Node;

struct ParseCSVTest : public ::testing::Test {

  void SetUp() {
    rmmInitialize(nullptr);
  }

};

const std::string content =
    R"(0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
6|FRANCE|3|refully final requests. regular, ironi
7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco
8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun
9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull
10|IRAN|4|efully alongside of the slyly final dependencies. 
11|IRAQ|4|nic deposits boost atop the quickly final requests? quickly regula
12|JAPAN|2|ously. final, express gifts cajole a
13|JORDAN|4|ic deposits are blithely about the carefully regular pa
14|KENYA|0| pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t
15|MOROCCO|0|rns. blithely bold courts among the closely regular packages use furiously bold platelets?
16|MOZAMBIQUE|0|s. ironic, unusual asymptotes wake blithely r
17|PERU|1|platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun
18|CHINA|2|c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos
19|ROMANIA|3|ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account
20|SAUDI ARABIA|4|ts. silent requests haggle. closely express packages sleep across the blithely
21|VIETNAM|2|hely enticingly express accounts. even, final 
22|RUSSIA|3| requests against the platelets use never according to the quickly regular pint
23|UNITED KINGDOM|3|eans boost carefully special requests. accounts are. carefull
24|UNITED STATES|1|y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be)";

TEST_F(ParseCSVTest, csv_with_strings) {
  std::cout << "csv_with_strings\n";
  std::string filename = "/tmp/nation.psv";
  std::ofstream outfile(filename, std::ofstream::out);
  outfile << content << std::endl;
  outfile.close();
  std::vector<std::string> names{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
  
  std::vector<std::string> files = {filename}; 

  cudf::csv_read_arg args(cudf::source_info{filename});
  args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
  args.dtype = { "int32", "str", "int32", "str" };
  args.header = -1;
  args.delimiter = '|';
  args.use_cols_names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};

  std::vector<Uri> uris;

  uris.push_back(Uri{filename});
  ral::io::Schema schema;
  auto parser = std::make_shared<ral::io::csv_parser>(args);
  auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

  { 
    ral::io::data_loader loader(parser, provider);
    try {
      loader.get_schema(schema, {});
      for (auto name : schema.get_names()) {
        std::cout << name << std::endl;
      }
      for (auto type : schema.get_types()) {
        std::cout << type << std::endl;
      }

    } catch (std::exception &e) {
      return;
    }
  }

  Context queryContext{0, std::vector<std::shared_ptr<Node>>(), std::shared_ptr<Node>(), ""};
  ral::io::data_loader loader(parser, provider);

  auto csv_table = loader.load_data(queryContext, {}, schema);
  if (csv_table != nullptr) {
    std::cout << "csv_table != nullptr\n";
    for (auto name : csv_table->names()) {
        std::cout << name << std::endl;
    }
  }

//   for (size_t column_index = 0; column_index < input_table.size();
//        column_index++) {
//     std::cout << "col_name: "
//               << input_table[column_index].get_gdf_column()->col_name << "|"
//               << input_table[column_index].get_gdf_column()->size << std::endl;
//     print_gdf_column(input_table[column_index].get_gdf_column());
//   }
}