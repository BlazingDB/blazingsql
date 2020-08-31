from blazingsql import BlazingContext
from blazingsql import DataType, S3EncryptionType
import numpy as np

import pandas as pd
import os
import subprocess
import sys


drill = "drill"
spark = "spark"
conda_prefix = os.getenv("CONDA_PREFIX")

bucket_name = os.getenv("BLAZINGSQL_E2E_AWS_S3_BUCKET_NAME")
access_key_id = os.getenv("BLAZINGSQL_E2E_AWS_S3_ACCESS_KEY_ID")
secret_key = os.getenv("BLAZINGSQL_E2E_AWS_S3_SECRET_KEY")

gs_project_id=os.getenv("BLAZINGSQL_E2E_GS_PROJECT_ID")
gs_bucket_name=os.getenv("BLAZINGSQL_E2E_GS_BUCKET_NAME")

file_results_dir = conda_prefix + "/blazingsql-testing-files/results/" 
data_dir = conda_prefix + "/blazingsql-testing-files/data/"

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def git_clone():
    import git
    git.Git("$CONDA_PREFIX").clone("https://github.com/BlazingDB/blazingsql-testing-files.git")
    
def git_pull():
    import git
    os.chdir(conda_prefix + "/blazingsql-testing-files")

def unzip():

    import tarfile
    
    os.chdir(conda_prefix + "/blazingsql-testing-files/")
    tar = tarfile.open('data.tar.gz', "r:gz")
    tar.extractall()
    tar.close()

def run_query(bc, engine, query, queryId, queryType, worder, orderBy,  acceptable_difference, use_percentage, input_type, **kwargs):
    
    result_gdf = bc.sql(query)       

    filename = str("TPCH").upper() + "-" + str(queryId) + ".parquet"

    result_file = file_results_dir + "/" + str(engine) + "/" + filename
    
    pdf2 = pd.read_parquet(result_file)

    stringResult = "" 
    
    if result_gdf is not None:
        if result_gdf.columns is not None:
            import dask_cudf
            
            if type(result_gdf) is dask_cudf.core.DataFrame:
                result_gdf = result_gdf.compute()
            
            expected_dtypes = result_gdf.dtypes.to_list()
            
            pdf = upcast_to_float(result_gdf).fillna(get_null_constants(result_gdf)).to_pandas()
            
            if worder == 1 and pdf.size != 0:
                pdf.sort_values([orderBy] if orderBy else pdf.columns.to_list(), inplace = True)

            stringResult = print_query_results(pdf, pdf2,  acceptable_difference, use_percentage, engine)

    return stringResult
          

def print_query_results(pdf1, pdf2,  acceptable_difference, use_percentage, engine):

    columnNamesComparison = compare_column_names(pdf1, pdf2)
    if columnNamesComparison != True:
        error_message = "Column names are not the same"
    
    resultComparisson = compare_results(pdf1, pdf2,  acceptable_difference, use_percentage, engine)
    if resultComparisson != "Success":
        error_message = resultComparisson[6:]

    stringResult = resultComparisson
    if resultComparisson != "Success" or columnNamesComparison == False:
        stringResult = "Fail"    
        
    return stringResult

def compare_column_names(pdf1, pdf2):
    if len(pdf1.columns) != len(pdf2.columns):
        if pdf1.values.size == 0 and pdf2.values.size == 0:
            return True
        print("Different set of columns")
        return False
    for blzCol, drillCol in zip(pdf1.columns.values.tolist(), pdf2.columns.values.tolist()):
        if blzCol != drillCol:
            if begins_with(drillCol, blzCol, "EXPR")==False and begins_with(drillCol, blzCol, "count(")==False:
                print("Different columns")
                return False
    return True

def begins_with(col1, col2, exp):
    return col1.startswith(exp) or col2.startswith(exp) 

def compare_results(vdf1, vdf2,  acceptable_difference, use_percentage, engine):
    if vdf1.size == 0 and vdf2.size == 0:
        return 'Success'
    elif pre_compare_results(vdf1.values, vdf2.values):
        return 'Success'
    else:
        res = assert_equal(vdf1, vdf2, acceptable_difference, use_percentage, engine) 
        return res
    
def upcast_to_float(df):
    for name in df.columns:
        if np.issubdtype(df[name].dtype, np.bool_):
            df[name] = df[name].astype(np.float32)
        elif np.issubdtype(df[name].dtype, np.integer):
            df[name] = df[name].astype(np.float64)
    return df

def get_null_constants(df):
    null_values = {}
    for col, dtype in df.dtypes.to_dict().items():
        if np.issubdtype(dtype, np.datetime64):
            null_values[col] = np.datetime64('nat')
        elif np.issubdtype(dtype, np.number):
            null_values[col] = np.nan
    return null_values

def pre_compare_results(vdf1, vdf2):
    try:
        np.testing.assert_equal(vdf1, vdf2)
        return True
    except (AssertionError, ValueError, TypeError) as e:
        return False
    
def assert_equal(pdf1, pdf2, acceptable_difference, use_percentage, engine):
    np.warnings.filterwarnings('ignore')
    if pdf1.shape[0] == pdf2.shape[0]:
        if pdf1.shape[1] == pdf2.shape[1]:

            pdf1.reset_index(drop=True, inplace=True)
            pdf2.reset_index(drop=True, inplace=True)

            exac_comp = pdf1.select_dtypes(exclude=np.inexact).equals(pdf2.select_dtypes(exclude=np.inexact))

            tmp_pdf1 = pdf1.select_dtypes(include=np.inexact)
            tmp_pdf2 = pdf2.select_dtypes(include=np.inexact)
            
            res = np.all(exac_comp) and np.allclose(tmp_pdf1.values, tmp_pdf2.values, acceptable_difference, equal_nan=True)
            if res:
                return  'Success'
            else:
                return  'Fail: Different values'
        else:
            return  'Fail: Different number of columns blzSQLresult: ' + str(pdf1.shape[1]) + ' ' + (engine) + ' result: ' + str(pdf2.shape[1])
    else:
        return  'Fail: Different number of rows blzSQLresult: ' + str(pdf1.shape[0]) + ' ' + (engine) + ' result: '+ str(pdf2.shape[0])

def create_tables(bc, dir_data_lc, fileSchemaType, **kwargs):
    
    ext = "parquet" 
    
    tpchTables = ['customer','orders','supplier','lineitem','part','partsupp','nation','region']

    tables = kwargs.get('tables', tpchTables)

    dir_data_lc = dir_data_lc + "tpch/"
    
    for i, table in enumerate(tables):
        table_files = ("%s/%s_[0-9]*.%s") % (dir_data_lc, table, ext)
        t = None
        t = bc.create_table(table, table_files)

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def main():
    
    #gitpython

    install("gitpython")

    #install("tarfile")
    
    if not os.path.exists(conda_prefix + "/blazingsql-testing-files/"):
        git_clone()
    else:
        git_pull()
    
    unzip()
    
    queryType = ' Local Tests ' 
    
    bc = BlazingContext()

    log_dict = {}
    
    def executionLocalTest(queryType): 
        
        #Read Data TPCH------------------------------------------------------------------------------------------------------------
        
        tables = ['nation', 'region', 'supplier','customer','lineitem','orders', 'part', 'partsupp']
        
        data_types =  [DataType.PARQUET] # TODO json
    
        for fileSchemaType in data_types:
            create_tables(bc, data_dir, fileSchemaType, tables=tables)

        #   Run Query -----------------------------------------------------------------------------
            worder = 1 # Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.01
                     
            print('==============================')
            print(queryType)
            print('==============================')
        
            queryId = 'TEST_01'
            print("Executing " + queryId + " ... ")
            query = """ select
                    l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,
                    sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
                    sum(l_quantity)/count(l_quantity) as avg_qty, sum(l_extendedprice)/count(l_extendedprice) as avg_price, sum(l_discount)/count(l_discount) as avg_disc,
                    count(*) as count_order
                from 
                    lineitem
                where
                    cast(l_shipdate as date) <= date '1998-09-01'
                group by
                    l_returnflag, l_linestatus
                order by
                    l_returnflag, l_linestatus"""            
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result
            
            queryId = 'TEST_02'
            print("Executing " + queryId + " ... ")
            query = """ select 
                        s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment
                    from 
                        supplier as s 
                    INNER JOIN nation as n ON s.s_nationkey = n.n_nationkey 
                    INNER JOIN partsupp as ps ON s.s_suppkey = ps.ps_suppkey
                    INNER JOIN part as p ON p.p_partkey = ps.ps_partkey 
                    INNER JOIN region as r ON r.r_regionkey = n.n_regionkey
                    where r.r_name = 'EUROPE' and p.p_size = 15
                        and p.p_type like '%BRASS'
                        and ps.ps_supplycost = (
                            select 
                                min(psq.ps_supplycost)
                            from 
                                partsupp as psq
                            INNER JOIN supplier as sq ON sq.s_suppkey = psq.ps_suppkey
                            INNER JOIN nation as nq ON nq.n_nationkey = sq.s_nationkey
                            INNER JOIN region as rq ON rq.r_regionkey = nq.n_regionkey
                            where
                                rq.r_name = 'EUROPE'
                        )
                    order by 
                        s.s_acctbal desc, n.n_name, s.s_name, p.p_partkey"""            
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_03'
            print("Executing " + queryId + " ... ")
            query = """ select 
                    l.l_orderkey, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue, o.o_orderdate, o.o_shippriority
                from 
                    orders as o
                INNER JOIN lineitem as l ON l.l_orderkey = o.o_orderkey
                INNER JOIN customer as c ON c.c_custkey = o.o_custkey
                where
                    c.c_mktsegment = 'BUILDING'
                    and o.o_orderdate < date '1995-03-15' 
                    and l.l_shipdate > date '1995-03-15'
                group by
                    l.l_orderkey, o.o_orderdate, o.o_shippriority
                order by
                    revenue desc, o.o_orderdate"""          
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_04'
            #print("Executing " + queryId + " ... ")
            query = """ select
                    o_orderpriority, count(*) as order_count
                from
                    orders
                where
                    o_orderdate >= date '1993-07-01'
                    and o_orderdate < date '1994-10-01'
                    and exists (select
                                    *
                                    from
                                    lineitem
                                    where
                                    l_orderkey = o_orderkey
                                    and l_commitdate < l_receiptdate)
                group by
                    o_orderpriority 
                order by 
                    o_orderpriority"""           
            #result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            queryId = 'TEST_05'
            print("Executing " + queryId + " ... ")
            query = """ select
                        n.n_name, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
                    from orders o 
                    inner join lineitem l on l.l_orderkey = o.o_orderkey
                    inner join customer c on o.o_custkey = c.c_custkey
                    inner join supplier s on l.l_suppkey = s.s_suppkey and c.c_nationkey = s.s_nationkey
                    inner join nation n on n.n_nationkey = s.s_nationkey
                    inner join region r on n.n_regionkey = r.r_regionkey
                    where
                        r.r_name = 'ASIA' 
                        and o.o_orderdate >= date '1994-01-01'
                        and o.o_orderdate < date '1995-01-01'                    
                    group by
                        n.n_name
                    order by
                        revenue desc"""            
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_06'
            print("Executing " + queryId + " ... ")
            query = """ select
                    sum(l_extendedprice*l_discount) as revenue
                from
                    lineitem
                where
                    l_shipdate >= date '1994-01-01' 
                    and l_shipdate < date '1995-01-01'
                    and l_discount between 0.05 and 0.07 
                    and l_quantity < 24"""            
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_07'
            print("Executing " + queryId + " ... ")
            query = """ select
                    supp_nation, cust_nation, l_year, sum(volume) as revenue
                from 
                    (select 
                        n1.n_name as supp_nation, n2.n_name as cust_nation,
                        extract(year from l.l_shipdate) as l_year, l.l_extendedprice * (1 - l.l_discount) as volume
                    from 
                        nation as n1
                    INNER JOIN supplier as s ON s.s_nationkey = n1.n_nationkey
                    INNER JOIN lineitem as l ON l.l_suppkey = s.s_suppkey 
                    INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey
                    INNER JOIN customer as c ON c.c_custkey = o.o_custkey
                    INNER JOIN nation as n2 ON n2.n_nationkey = c.c_nationkey
                    where
                        (
                            (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') 
                        )
                    and cast(l.l_shipdate as date) between date '1995-01-01' and date '1996-12-31') as shipping
                group by
                    supp_nation, cust_nation, l_year
                order by 
                    supp_nation, cust_nation, l_year
                    """            
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

    executionLocalTest(queryType)
    
    queryType = ' S3 Tests ' 
    
    def executionS3Test(queryType): 
        
        #Read Data TPCH------------------------------------------------------------------------------------------------------------
        
        authority = "tpch_s3"
        
        bc.s3(authority, bucket_name=bucket_name, encryption_type=S3EncryptionType.NONE,
              access_key_id=access_key_id, secret_key=secret_key)
        
        dir_data_lc = "s3://" + authority + "/" + "DataSet100Mb2part/" 
        
        tables = ['nation', 'region', 'supplier','customer','lineitem','orders', 'part', 'partsupp']
        data_types =  [DataType.PARQUET] # TODO json
    
        for fileSchemaType in data_types:
            create_tables(bc, data_dir, fileSchemaType, tables=tables)

        #   Run Query -----------------------------------------------------------------------------
            worder = 1 # Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.01
                     
            print('==============================')
            print(queryType)
            print('==============================')
        
            queryId = 'TEST_08'
            print("Executing " + queryId + " ... ")
            query = """ select 
                    o_year, sum(case when nationl = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
                from (
                    select 
                        extract(year from o.o_orderdate) as o_year, l.l_extendedprice * (1 - l.l_discount) as volume, n2.n_name as nationl
                    from 
                        part as p
                    INNER JOIN lineitem as l ON p.p_partkey = l.l_partkey
                    INNER JOIN supplier as s ON s.s_suppkey = l.l_suppkey
                    INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey 
                    INNER JOIN customer as c ON c.c_custkey = o.o_custkey
                    INNER JOIN nation as n1 ON n1.n_nationkey = c.c_nationkey 
                    INNER JOIN region as r ON r.r_regionkey = n1.n_regionkey
                    INNER JOIN nation as n2 ON n2.n_nationkey = s.s_nationkey
                    where 
                        r.r_name = 'AMERICA' 
                        and o.o_orderdate >= date '1995-01-01' and o.o_orderdate <= date '1996-12-31'
                        and p.p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
                group by
                    o_year
                order by
                    o_year"""          
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_09'
            #print("Executing " + queryId + " ... ")
            query = """ select
                    nationl, o_year, sum(amount) as sum_profit
                from
                    ( select n_name as nationl, extract(year from o_orderdate) as o_year,
                            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                        from lineitem
                        INNER JOIN orders ON o_orderkey = l_orderkey
                        INNER JOIN partsupp ON ps_suppkey = l_suppkey and ps_partkey = l_partkey
                        INNER JOIN part ON p_partkey = l_partkey
                        INNER JOIN supplier ON s_suppkey = l_suppkey
                        INNER JOIN nation ON n_nationkey = s_nationkey                     
                        where
                            p_name like '%green%' ) as profit
                group by 
                    nationl, o_year 
                order by 
                    nationl, o_year desc"""           
            #result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            queryId = 'TEST_10'
            print("Executing " + queryId + " ... ")
            query = """ select
                    c.c_custkey, c.c_name, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue, c.c_acctbal,
                    n.n_name, c.c_address, c.c_phone, c.c_comment
                from
                    customer as c
                INNER JOIN nation as n ON n.n_nationkey = c.c_nationkey
                INNER JOIN orders as o ON o.o_custkey = c.c_custkey
                INNER JOIN lineitem as l ON l.l_orderkey = o.o_orderkey
                where 
                o.o_orderdate >= date '1993-10-01'
                and o.o_orderdate < date '1994-10-01'
                and l.l_returnflag = 'R'
                group by
                    c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment"""            
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_11'
            #print("Executing " + queryId + " ... ")
            query = """ select
                            ps.ps_partkey, sum(ps.ps_supplycost * ps.ps_availqty) as valuep
                        from 
                            partsupp as ps
                        INNER JOIN supplier as s ON ps.ps_suppkey = s.s_suppkey
                        INNER JOIN nation as n ON s.s_nationkey = n.n_nationkey
                        where
                            n.n_name = 'GERMANY'
                        group by 
                            ps.ps_partkey having sum(ps.ps_supplycost * ps.ps_availqty) > ( select
                                                                        sum(psq.ps_supplycost)
                                                                        from
                                                                            partsupp as psq
                                                                        INNER JOIN supplier as sq ON psq.ps_suppkey = sq.s_suppkey
                                                                        INNER JOIN nation as nq ON sq.s_nationkey = nq.n_nationkey
                                                                        where
                                                                            nq.n_name = 'GERMANY'
                                                                        )
                        order by 
                            valuep desc"""           
            #result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            queryId = 'TEST_12'
            print("Executing " + queryId + " ... ")
            query = """ select
                    l.l_shipmode, sum(case when o.o_orderpriority ='1-URGENT' or o.o_orderpriority ='2-HIGH'
                    then 1 else 0 end) as high_line_count, sum(case when o.o_orderpriority <> '1-URGENT'
                    and o.o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
                from
                    lineitem as l
                INNER JOIN orders as o ON o.o_orderkey = l.l_orderkey
                where
                    l.l_shipmode in ('MAIL', 'SHIP') 
                    and l.l_commitdate < l.l_receiptdate
                    and l.l_shipdate < l.l_commitdate 
                    and l.l_receiptdate >= date '1994-01-01'
                    and l.l_receiptdate < date '1995-01-01'
                group by l.l_shipmode
                order by l.l_shipmode"""          
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_13'
            print("Executing " + queryId + " ... ")
            query = """ select
                    c_count, count(*) as custdist
                from
                    (select
                        c.c_custkey, count(o.o_orderkey)
                    from
                        customer as c
                    LEFT OUTER JOIN orders as o ON c.c_custkey = o.o_custkey
                    where o.o_comment not like '%special%requests%'
                    group by
                        c.c_custkey) as c_orders (c_custkey, c_count)
                group by
                    c_count
                order by
                    custdist desc, c_count desc"""           
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_14'
            print("Executing " + queryId + " ... ")
            query = """ select 100.00 * sum( case when p.p_type like 'PROMO%' then l.l_extendedprice * (1 - l.l_discount)
                                    else 0 end) / sum(l.l_extendedprice * (1 - l.l_discount) ) as promo_revenue
                from 
                    lineitem as l
                INNER JOIN part as p ON p.p_partkey = l.l_partkey
                where
                    l.l_shipdate >= date '1995-09-01' 
                    and l.l_shipdate < date '1995-10-01'"""          
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)

            log_dict[queryId] = result

    executionS3Test(queryType)
    
    queryType = ' GS Tests ' 
    
    def executionGSTest(queryType): 
        
        authority = "tpch_gs"
        
        bc.gs(authority,
        project_id=gs_project_id,
        bucket_name=gs_bucket_name,
        use_default_adc_json_file=True,
        adc_json_file='')
        
        dir_data_lc = 'gcs://'+ authority +'/100MB2Part/'
        
        tables = ['nation', 'region', 'supplier','customer','lineitem','orders', 'part', 'partsupp']
        data_types =  [DataType.PARQUET] 
    
        for fileSchemaType in data_types:
            create_tables(bc, data_dir, fileSchemaType, tables=tables)

        #   Run Query -----------------------------------------------------------------------------
            worder = 1 # Parameter to indicate if its necessary to order the resulsets before compare them
            use_percentage = False
            acceptable_difference = 0.01
                     
            print('==============================')
            print(queryType)
            print('==============================')
        
            queryId = 'TEST_15'
            print("Executing " + queryId + " ... ")
            query = """with revenue (suplier_no, total_revenue) as
                (
                select
                l_suppkey, cast(sum(l_extendedprice * (1-l_discount)) as int)
                from
                lineitem
                where
                l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'
                group by
                l_suppkey
                )
                select
                s.s_suppkey, s.s_name, s.s_address, s.s_phone, re.total_revenue
                from
                supplier as s
                INNER JOIN revenue as re on s.s_suppkey = re.suplier_no
                where
                re.total_revenue = cast((select max(total_revenue) from revenue) as int)
                order by
                s.s_suppkey"""        
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_16'
            #print("Executing " + queryId + " ... ")
            query = """ select
                    p.p_brand, p.p_type, p.p_size, count(distinct ps.ps_suppkey) as supplier_cnt
                from partsupp ps
                inner join part p on p.p_partkey = ps.ps_partkey
                where
                    p.p_brand <> 'Brand#45'
                    and p.p_type not like 'MEDIUM POLISHED%' and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
                    and ps.ps_suppkey not in (select s.s_suppkey from supplier s where s.s_comment like '%Customer%Complaints%')
                group by
                    p.p_brand, p.p_type, p.p_size
                order by
                    supplier_cnt desc, p.p_brand, p.p_type, p.p_size"""         
            #result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            queryId = 'TEST_17'
            print("Executing " + queryId + " ... ")
            query = """ select
                    sum(l1.l_extendedprice) / 7.0 as avg_yearly
                from lineitem l1
                inner join part p on p.p_partkey = l1.l_partkey
                where
                    p.p_brand = 'Brand#23' and p.p_container = 'MED BOX'
                    and l1.l_quantity < (select 0.2 * avg(l2.l_quantity) from lineitem l2
                                    where l2.l_partkey = p.p_partkey)"""        
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_18'
            print("Executing " + queryId + " ... ")
            query = """ select
                    c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice, sum(l.l_quantity)
                from
                    customer c
                    inner join orders o on c.c_custkey = o.o_custkey
                    inner join lineitem l on o.o_orderkey = l.l_orderkey
                where
                    o.o_orderkey in (select l2.l_orderkey from lineitem l2 group by l2.l_orderkey having
                                    sum(l2.l_quantity) > 300)
                group by
                    c.c_name, c.c_custkey, o.o_orderkey, o.o_orderdate, o.o_totalprice
                order by
                    o.o_totalprice desc, o.o_orderdate"""          
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_19'
            print("Executing " + queryId + " ... ")
            query = """ select 
                    sum(l.l_extendedprice * (1 - l.l_discount) ) as revenue
                from 
                    lineitem as l
                INNER JOIN part as p ON l.l_partkey = p.p_partkey
                where
                    (
                    p.p_brand = 'Brand#12'
                    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    and l.l_quantity >= 1 and l.l_quantity <= 11
                    and p.p_size between 1 and 5
                    and l.l_shipmode in ('AIR', 'AIR REG')
                    and l.l_shipinstruct = 'DELIVER IN PERSON'
                    )
                    or
                    (
                    p.p_brand = 'Brand#23'
                    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    and l.l_quantity >= 10 and l.l_quantity <= 20
                    and p.p_size between 1 and 10
                    and l.l_shipmode in ('AIR', 'AIR REG')
                    and l.l_shipinstruct = 'DELIVER IN PERSON'
                    )
                    or
                    (
                    p.p_brand = 'Brand#34'
                    and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    and l.l_quantity >= 20 and l.l_quantity <= 30
                    and p.p_size between 1 and 15
                    and l.l_shipmode in ('AIR', 'AIR REG')
                    and l.l_shipinstruct = 'DELIVER IN PERSON'
                    )"""         
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_20'
            print("Executing " + queryId + " ... ")
            query = """ select s.s_name, s.s_address
                    from supplier s
                inner join nation n on s.s_nationkey = n.n_nationkey
                where
                    s.s_suppkey in (select ps.ps_suppkey from partsupp ps where ps.ps_partkey in (select p.p_partkey
                                                                                        from part p where
                                                                                    p.p_name like 'forest%')
                                and ps_availqty > (select 0.5 * sum(l.l_quantity) from lineitem l where
                                                    l.l_partkey = ps.ps_partkey and l.l_suppkey = ps.ps_suppkey
                                                    and l.l_shipdate >= date '1994-01-01' 
                                                    and l.l_shipdate < date '1995-01-01'))
                    and n.n_name = 'CANADA'
                order by s.s_name"""             
            result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_21'
            print("Executing " + queryId + " ... ")
            query = """ select
                    s_name, count(*) as numwait
                from
                    supplier
                    inner join lineitem l1 on s_suppkey = l1.l_suppkey
                    inner join orders on o_orderkey = l1.l_orderkey
                    inner join nation on s_nationkey = n_nationkey
                where
                    o_orderstatus = 'F'
                    and l1.l_receiptdate > l1.l_commitdate and exists (select * from lineitem l2
                                                                        where l2.l_orderkey = l1.l_orderkey
                                                                            and l2.l_suppkey <> l1.l_suppkey)
                    and not exists (select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey
                                    and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate)
                    and n_name = ' SAUDI ARABIA'
                group by
                    s_name
                order by
                    numwait desc, s_name"""        
            result = run_query(bc, spark, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
            
            log_dict[queryId] = result

            queryId = 'TEST_22'
            #print("Executing " + queryId + " ... ")
            query = """ select
                    cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
                from (select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer
                    where substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17')
                    and c_acctbal > (select avg(c_acctbal) from customer where c_acctbal > 0.00
                    and substring (c_phone from 1 for 2) in ('13','31','23','29','30','18','17'))
                    and not exists (select * from orders where o_custkey = c_custkey)) as custsale
                group by cntrycode
                order by cntrycode"""        
            #result = run_query(bc, drill, query, queryId, queryType, worder, '', acceptable_difference, use_percentage, fileSchemaType)
    
    executionGSTest(queryType)

    green = bcolors.OKGREEN
    endc = bcolors.ENDC

    print(green + "=======================================")
    print("SUMMARY TESTS")
    print("=======================================" + endc)

    for key, value in log_dict.items():
	    print('"{}" : {} '.format(key, value))

    print (green + "=======================================" + endc)

main()
    
    
