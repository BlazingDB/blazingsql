#include "file_generator.h"

#include "io/DataLoader.h"
#include <fstream>

namespace blazingdb {
namespace test {
using data_provider_pair = std::pair<std::shared_ptr<ral::io::csv_parser>, std::shared_ptr<ral::io::uri_data_provider>>;


data_provider_pair CreateCsvCustomerTableProvider(int index) {
	const std::string content =
		R"(1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e
    2|Customer#000000002|XSTf4,NCwDVaWNe6tEgvwfmRchLXak|13|23-768-687-3665|121.65|AUTOMOBILE|l accounts. blithely ironic theodolites integrate boldly: caref
    3|Customer#000000003|MG9kdTD2WBHm|1|11-719-748-3364|7498.12|AUTOMOBILE| deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov
    4|Customer#000000004|XxVSJsLAGtn|4|14-128-190-5944|2866.83|MACHINERY| requests. final, regular ideas sleep final accou
    5|Customer#000000005|KvpyuHCplrB84WgAiGV6sYpZq7Tj|3|13-750-942-6364|794.47|HOUSEHOLD|n accounts will have to unwind. foxes cajole accor
    6|Customer#000000006|sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn|20|30-114-968-4951|7638.57|AUTOMOBILE|tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious
    7|Customer#000000007|TcGe5gaZNgVePxU5kRrvXBfkasDTea|18|28-190-982-9759|9561.95|AUTOMOBILE|ainst the ironic, express theodolites. express, even pinto beans among the exp
    8|Customer#000000008|I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5|17|27-147-574-9335|6819.74|BUILDING|among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide
    9|Customer#000000009|xKiAFTjUsCuxfeleNqefumTrjS|8|18-338-906-3675|8324.07|FURNITURE|r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl
    10|Customer#000000010|6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2|5|15-741-346-9870|2753.54|HOUSEHOLD|es regular deposits haggle. fur
    11|Customer#000000011|PkWS 3HlXqwTuzrKg633BEi|23|33-464-151-3439|-272.6|BUILDING|ckages. requests sleep slyly. quickly even pinto beans promise above the slyly regular pinto beans.
    12|Customer#000000012|9PWKuhzT4Zr1Q|13|23-791-276-1263|3396.49|HOUSEHOLD| to the carefully final braids. blithely regular requests nag. ironic theodolites boost quickly along
    13|Customer#000000013|nsXQu0oVjD7PM659uC3SRSp|3|13-761-547-5974|3857.34|BUILDING|ounts sleep carefully after the close frays. carefully bold notornis use ironic requests. blithely
    14|Customer#000000014|KXkletMlL2JQEA |1|11-845-129-3851|5266.3|FURNITURE|, ironic packages across the unus
    15|Customer#000000015|YtWggXoOLdwdo7b0y,BZaGUQMLJMX1Y,EC,6Dn|23|33-687-542-7601|2788.52|HOUSEHOLD| platelets. regular deposits detect asymptotes. blithely unusual packages nag slyly at the fluf
    16|Customer#000000016|cYiaeMLZSMAOQ2 d0W,|10|20-781-609-3107|4681.03|FURNITURE|kly silent courts. thinly regular theodolites sleep fluffily after
    17|Customer#000000017|izrh 6jdqtp2eqdtbkswDD8SG4SzXruMfIXyR7|2|12-970-682-3487|6.34|AUTOMOBILE|packages wake! blithely even pint
    18|Customer#000000018|3txGO AiuFux3zT0Z9NYaFRnZt|6|16-155-215-1315|5494.43|BUILDING|s sleep. carefully even instructions nag furiously alongside of t
    19|Customer#000000019|uc,3bHIx84H,wdrmLOjVsiqXCq2tr|18|28-396-526-5053|8914.71|HOUSEHOLD| nag. furiously careful packages are slyly at the accounts. furiously regular in
    20|Customer#000000020|JrPk8Pqplj4Ne|22|32-957-234-8742|7603.4|FURNITURE|g alongside of the special excuses-- fluffily enticing packages wake)";

	std::string filename = "/tmp/customer_" + std::to_string(index) + ".psv";
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();

	std::vector<std::pair<std::string, std::string>> customer_map = {{"c_custkey", "int64"},
		{"c_name", "str"},
		{"c_address", "str"},
		{"c_nationkey", "int64"},
		{"c_phone", "str"},
		{"c_acctbal", "float64"},
		{"c_mktsegment", "str"},
		{"c_comment", "str"}};
	std::vector<std::string> col_names;
	std::vector<std::string> dtypes;
	for(auto pair : customer_map) {
		col_names.push_back(pair.first);
		dtypes.push_back(pair.second);
	}
	cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
	in_args.names = col_names;
	in_args.dtype = dtypes;
	in_args.delimiter = '|';
	in_args.header = -1;

	std::vector<Uri> uris;
	uris.push_back(Uri{filename});

	auto parser = std::make_shared<ral::io::csv_parser>(in_args);
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	return std::make_pair(parser, provider);
}

data_provider_pair CreateCsvOrderTableProvider(int index) {
	const std::string content =
		R"(1|3691|O|194029.55|1996-01-02T00:00:00.000Z|5-LOW|Clerk#000000951|0|nstructions sleep furiously among
    2|7801|O|60951.63|1996-12-01T00:00:00.000Z|1-URGENT|Clerk#000000880|0| foxes. pending accounts at the pending, silent asymptot
    3|12332|F|247296.05|1993-10-14T00:00:00.000Z|5-LOW|Clerk#000000955|0|sly final accounts boost. carefully regular ideas cajole carefully. depos
    4|13678|O|53829.87|1995-10-11T00:00:00.000Z|5-LOW|Clerk#000000124|0|sits. slyly regular warthogs cajole. regular, regular theodolites acro
    5|4450|F|139660.54|1994-07-30T00:00:00.000Z|5-LOW|Clerk#000000925|0|quickly. bold deposits sleep slyly. packages use slyly
    6|5563|F|65843.52|1992-02-21T00:00:00.000Z|4-NOT SPECIFIED|Clerk#000000058|0|ggle. special, final requests are against the furiously specia
    7|3914|O|231037.28|1996-01-10T00:00:00.000Z|2-HIGH|Clerk#000000470|0|ly special requests
    32|13006|O|166802.63|1995-07-16T00:00:00.000Z|2-HIGH|Clerk#000000616|0|ise blithely bold, regular requests. quickly unusual dep
    33|6697|F|118518.56|1993-10-27T00:00:00.000Z|3-MEDIUM|Clerk#000000409|0|uriously. furiously final request
    34|6101|O|75662.77|1998-07-21T00:00:00.000Z|3-MEDIUM|Clerk#000000223|0|ly final packages. fluffily final deposits wake blithely ideas. spe
    35|12760|O|192885.43|1995-10-23T00:00:00.000Z|4-NOT SPECIFIED|Clerk#000000259|0|zzle. carefully enticing deposits nag furio
    36|11527|O|72196.43|1995-11-03T00:00:00.000Z|1-URGENT|Clerk#000000358|0| quick packages are blithely. slyly silent accounts wake qu
    37|8612|F|156440.15|1992-06-03T00:00:00.000Z|3-MEDIUM|Clerk#000000456|0|kly regular pinto beans. carefully unusual waters cajole never
    38|12484|O|64695.26|1996-08-21T00:00:00.000Z|4-NOT SPECIFIED|Clerk#000000604|0|haggle blithely. furiously express ideas haggle blithely furiously regular re
    39|8177|O|307811.89|1996-09-20T00:00:00.000Z|3-MEDIUM|Clerk#000000659|0|ole express, ironic requests: ir
    64|3212|F|30616.9|1994-07-16T00:00:00.000Z|3-MEDIUM|Clerk#000000661|0|wake fluffily. sometimes ironic pinto beans about the dolphin
    65|1627|P|99763.79|1995-03-18T00:00:00.000Z|1-URGENT|Clerk#000000632|0|ular requests are blithely pending orbits-- even requests against the deposit)";

	std::string filename = "/tmp/orders_" + std::to_string(index) + ".psv";
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();

	std::vector<std::pair<std::string, std::string>> orders_map = {{"o_orderkey", "int64"},
		{"o_custkey", "int64"},
		{"o_orderstatus", "str"},
		{"o_totalprice", "float64"},
		{"o_orderdatetime64", "str"},
		{"o_orderpriority", "str"},
		{"o_clerk", "str"},
		{"o_shippriority", "str"},
		{"o_comment", "str"}};

	std::vector<std::string> col_names;
	std::vector<std::string> dtypes;
	for(auto pair : orders_map) {
		col_names.push_back(pair.first);
		dtypes.push_back(pair.second);
	}
	cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
	in_args.names = col_names;
	in_args.dtype = dtypes;
	in_args.delimiter = '|';
	in_args.header = -1;

	std::vector<Uri> uris;
	uris.push_back(Uri{filename});

	auto parser = std::make_shared<ral::io::csv_parser>(in_args);
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	return std::make_pair(parser, provider);
}

data_provider_pair CreateCsvNationTableProvider(int index) {
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
9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully
)";

	std::string filename = "/tmp/nation_" + std::to_string(index) + ".psv";
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();

	std::vector<std::pair<std::string, std::string>> columns_map = {
        {"n_nationkey", "int32"},
		{"n_name", "str"},
		{"n_regionkey", "int32"},
		{"n_comment", "str"}};

	std::vector<std::string> col_names;
	std::vector<std::string> dtypes;
	for(auto pair : columns_map) {
		col_names.push_back(pair.first);
		dtypes.push_back(pair.second);
	}
	cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
	in_args.names = col_names;
	in_args.dtype = dtypes;
	in_args.delimiter = '|';
	in_args.header = -1;

	std::vector<Uri> uris;
	uris.push_back(Uri{filename});

	auto parser = std::make_shared<ral::io::csv_parser>(in_args);
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	return std::make_pair(parser, provider);
}


data_parquet_provider_pair CreateParquetCustomerTableProvider(Context * context, int n_batches) {
    std::vector<Uri> uris;
    std::vector<std::vector<int>> all_row_groups{};

    for (int index = 0; index < n_batches; index++) {
        auto provider = CreateCsvCustomerTableProvider(index);
        ral::io::data_loader loader(provider.first, provider.second);
        ral::io::Schema schema;
	    loader.get_schema(schema, {});
		auto local_cur_data_handle = provider.second->get_next();
		auto table = loader.load_batch(context, {}, schema, local_cur_data_handle, 0, {});
        std::string filepath = "/tmp/customer_" + std::to_string(index) + ".parquet";
        cudf_io::write_parquet_args out_args{cudf_io::sink_info{filepath}, table->view()};
        cudf_io::write_parquet(out_args);

        uris.push_back(Uri{filepath});
        std::vector<int> row_group{0};
        all_row_groups.push_back(row_group);
    }
    auto parser = std::make_shared<ral::io::parquet_parser>();
    auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::Schema tableSchema;
    ral::io::data_loader loader(parser, provider);
	loader.get_schema(tableSchema, {});
	ral::io::Schema schema(tableSchema.get_names(),
						  tableSchema.get_calcite_to_file_indices(),
						  tableSchema.get_dtypes(),
						  tableSchema.get_in_file(),
						  all_row_groups); 
	return std::make_tuple(parser, provider, schema);   
}

data_parquet_provider_pair CreateParquetOrderTableProvider(Context * context, int n_batches){
    std::vector<Uri> uris;
    std::vector<std::vector<int>> all_row_groups{};

    for (int index = 0; index < n_batches; index++) {
        auto provider = CreateCsvOrderTableProvider(index);
        ral::io::data_loader loader(provider.first, provider.second);
        ral::io::Schema schema;
	    loader.get_schema(schema, {});
		auto local_cur_data_handle = provider.second->get_next();
		auto table = loader.load_batch(context, {}, schema, local_cur_data_handle, 0, {});
        std::string filepath = "/tmp/orders_" + std::to_string(index) + ".parquet";
        cudf_io::write_parquet_args out_args{cudf_io::sink_info{filepath}, table->view()};
        cudf_io::write_parquet(out_args);

        uris.push_back(Uri{filepath});
        std::vector<int> row_group{0};
        all_row_groups.push_back(row_group);
    }
    auto parser = std::make_shared<ral::io::parquet_parser>();
    auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::Schema tableSchema;
    ral::io::data_loader loader(parser, provider);
	loader.get_schema(tableSchema, {});
	ral::io::Schema schema(tableSchema.get_names(),
						  tableSchema.get_calcite_to_file_indices(),
						  tableSchema.get_dtypes(),
						  tableSchema.get_in_file(),
						  all_row_groups); 
	return std::make_tuple(parser, provider, schema);   
}

data_parquet_provider_pair CreateParquetNationTableProvider(Context * context, int n_batches) {
    std::vector<Uri> uris;
    std::vector<std::vector<int>> all_row_groups{};

	for (int index = 0; index < n_batches; index++) {
	    auto provider = CreateCsvNationTableProvider(index);
	    ral::io::data_loader loader(provider.first, provider.second);
	    ral::io::Schema schema;
	    loader.get_schema(schema, {});
		auto local_cur_data_handle = provider.second->get_next();
		auto table = loader.load_batch(context, {}, schema, local_cur_data_handle, 0, {});
	    std::string filepath = "/tmp/nation_" + std::to_string(index) + ".parquet";
        cudf_io::write_parquet_args out_args{cudf_io::sink_info{filepath}, table->view()};
        cudf_io::write_parquet(out_args);
	
        uris.push_back(Uri{filepath});
        std::vector<int> row_group{0};
        all_row_groups.push_back(row_group);
    }
    auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::Schema tableSchema;
    ral::io::data_loader loader(parser, provider);
	loader.get_schema(tableSchema, {});
	ral::io::Schema schema(tableSchema.get_names(),
						  tableSchema.get_calcite_to_file_indices(),
						  tableSchema.get_dtypes(),
						  tableSchema.get_in_file(),
						  all_row_groups); 
	return std::make_tuple(parser, provider, schema);
}


}  // namespace test
}  // namespace blazingdb