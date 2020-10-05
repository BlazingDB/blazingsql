from Configuration import Settings as Settings
from DataBase import createSchema as cs
from pyblazing import EncryptionType
from pynvml import nvmlInit
from Runner import runTest
from Utils import Execution, gpuMemory, init_context


def main(drill, dir_data_lc, bc, nRals):

    start_mem = gpuMemory.capture_gpu_memory_usage()

    queryType = "Mixed filesystem Test"

    def executionTest(queryType):
        # Register HDFS
        bc.hdfs("35.185.48.245", host="35.185.48.245", port=54310,
                user="hadoop")
        dir_df = dir_data_lc[dir_data_lc.find("DataSet"): len(dir_data_lc)]
        dir_data_fs_hdfs = "hdfs://35.185.48.245/" + dir_df

        # Register S3
        authority = "tpch_s3"
        # TODO percy kharo e2e-gpuci security
        bc.s3(
            authority,
            bucket_name="blazingsql-bucket",
            encryption_type=EncryptionType.NONE,
            access_key_id="",
            secret_key="",
        )
        dir_df = dir_data_lc[dir_data_lc.find("DataSet"): len(dir_data_lc)]
        dir_data_fs_s3 = "s3://" + authority + "/" + dir_df

        # parque local
        region_files = cs.get_filenames_for_table("region", dir_data_lc,
                                                  "parquet", "")
        bc.create_table("region", region_files)
        # csv local
        nation_files = cs.get_filenames_for_table("nation", dir_data_lc,
                                                  "psv", "")
        bc.create_table(
            "nation",
            nation_files,
            delimiter="|",
            dtype=cs.get_dtypes("nation"),
            names=cs.get_column_names("nation"),
        )
        # parquet from hdfs
        lineitem_files = cs.get_filenames_for_table(
            "lineitem", dir_data_lc, "parquet", dir_data_fs_hdfs
        )
        bc.create_table("lineitem", lineitem_files)
        # parquet from S3
        customer_files = cs.get_filenames_for_table(
            "customer", dir_data_lc, "parquet", ""
        )
        bc.create_table("customer", customer_files)
        # csv con hdfs
        supplier_files = cs.get_filenames_for_table(
            "supplier", dir_data_lc, "psv", dir_data_fs_hdfs
        )
        bc.create_table(
            "supplier",
            supplier_files,
            delimiter="|",
            dtype=cs.get_dtypes("supplier"),
            names=cs.get_column_names("supplier"),
        )
        # csv from S3
        orders_files = cs.get_filenames_for_table(
            "orders", dir_data_lc, "psv", dir_data_fs_s3
        )
        bc.create_table(
            "orders",
            orders_files,
            delimiter="|",
            dtype=cs.get_dtypes("orders"),
            names=cs.get_column_names("orders"),
        )

        # Run Query ------------------------------------------------------
        # Parameter to indicate if its necessary to order
        # the resulsets before compare them
        worder = 1
        use_percentage = False
        acceptable_difference = 0.01

        print("==============================")
        print(queryType)
        print("==============================")

        queryId = "TEST_01"  # parquet-s3, csv-local, parque-local
        query = """select sum(c.c_custkey)/count(c.c_custkey),
                    sum(c.c_acctbal)/count(c.c_acctbal),
                    n.n_nationkey, r.r_regionkey
                from customer as c
                inner join nation as n on c.c_nationkey = n.n_nationkey
                inner join region as r on r.r_regionkey = n.n_regionkey
                group by n.n_nationkey, r.r_regionkey"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            True,
        )  # TODO: Change sum/count for avg KC

        queryId = "TEST_02"  # parquet-s3, csv-s3
        query = """select sum(custKey)/count(custKey) from
        (select customer.c_custkey as custKey from
        (select min(o_custkey) as o_custkey from orders ) as tempOrders
        inner join customer on tempOrders.o_custkey = customer.c_custkey
        where customer.c_nationkey > 6) as joinedTables"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            True,
        )  # TODO: Change sum/count for avg KC

        queryId = "TEST_03"  # csv-s3, parquet-hdfs
        query = """select o.o_orderkey, o.o_totalprice,
                l.l_partkey, l.l_returnflag from orders as o
                inner join lineitem as l
                on o.o_orderkey = l.l_orderkey
                where l.l_orderkey < 1000"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            use_percentage,
            # fileSchemaType,
        )

        queryId = "TEST_04"  # csv-local, parquet-local, parquet-s3
        query = """select sum(c.c_custkey)/count(c.c_custkey),
                    sum(c.c_acctbal)/count(c.c_acctbal), n.n_nationkey,
                    r.r_regionkey
                from customer as c
                inner join nation as n on c.c_nationkey = n.n_nationkey
                inner join region as r on r.r_regionkey = n.n_regionkey
                group by n.n_nationkey, r.r_regionkey"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            True,
        )  # TODO: Change sum/count for avg KC

        queryId = "TEST_05"  # parquet-s3, csv-local
        query = """select c.c_custkey, c.c_nationkey, n.n_regionkey
                from customer as c
                inner join nation as n on c.c_nationkey = n.n_nationkey
                where n.n_regionkey = 1 and c.c_custkey < 50"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            use_percentage,
            # fileSchemaType,
        )

        queryId = "TEST_06"  # csv-hdfs, parquet-hdfs, parquet-s3, csv-local
        query = """select n1.n_nationkey as supp_nation,
                    n2.n_nationkey as cust_nation,
                    l.l_extendedprice * l.l_discount
                from supplier as s
                inner join lineitem as l on s.s_suppkey = l.l_suppkey
                inner join orders as o on o.o_orderkey = l.l_orderkey
                inner join customer as c on c.c_custkey = o.o_custkey
                inner join nation as n1 on s.s_nationkey = n1.n_nationkey
                inner join nation as n2 on c.c_nationkey = n2.n_nationkey
                where n1.n_nationkey = 1
                and n2.n_nationkey = 2 and o.o_orderkey < 10000"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            use_percentage,
            # fileSchemaType,
        )

        queryId = "TEST_07"  # csv-local, parquet-local
        query = """with regionTemp as (
                select r_regionkey, r_name from region where r_regionkey > 2
                ), nationTemp as(
                    select n_nationkey, n_regionkey as fkey, n_name
                    from nation where n_nationkey > 3
                    order by n_nationkey
                )
                select regionTemp.r_name, nationTemp.n_name
                from regionTemp inner join nationTemp
                on regionTemp.r_regionkey = nationTemp.fkey"""
        runTest.run_query(
            bc,
            drill,
            query,
            queryId,
            queryType,
            worder,
            "",
            acceptable_difference,
            use_percentage,
            # fileSchemaType,
        )

    executionTest(queryType)

    end_mem = gpuMemory.capture_gpu_memory_usage()

    gpuMemory.log_memory_usage(queryType, start_mem, end_mem)


if __name__ == "__main__":

    Execution.getArgs()

    nvmlInit()

    compare_results = True
    if "compare_results" in Settings.data["RunSettings"]:
        compare_results = Settings.data["RunSettings"]["compare_results"]

    drill = None

    if compare_results:
        # Create Table Drill ------------------------------------------------
        from pydrill.client import PyDrill

        drill = PyDrill(host="localhost", port=8047)
        cs.init_drill_schema(drill,
                             Settings.data["TestSettings"]["dataDirectory"])

    # Create Context For BlazingSQL
    bc, dask_client = init_context()

    nRals = Settings.data["RunSettings"]["nRals"]

    main(dask_client, drill, Settings.data["TestSettings"]["dataDirectory"],
         bc, nRals)

    runTest.save_log()

    gpuMemory.print_log_gpu_memory()
