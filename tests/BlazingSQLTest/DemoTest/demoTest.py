# if use_registered_hdfs:
#     acq_data_path = "hdfs://myLocalHdfs/data/acq"
#     perf_data_path = "hdfs://myLocalHdfs/data/perf"
#     col_names_path = "hdfs://myLocalHdfs/data/names.csv"
# elif use_registered_posix:
#     acq_data_path = "file://mortgage/data/acq"
#     perf_data_path = "file://mortgage/data/perf"
#     col_names_path = "file://mortgage/data/names.csv"
# else:
# import os

import time
import pandas as pd
import pyblazing
from pyblazing import DriverType, FileSystemType
from sklearn.model_selection import train_test_split

import xgboost as xgb
from Configuration import Settings as Settings
from DataBase import createSchema
from DemoTest.chronometer import Chronometer
from Utils import Execution


def register_hdfs():
    print("*** Register a HDFS File System ***")
    fs_status = pyblazing.register_file_system(
        authority="myLocalHdfs",
        type=FileSystemType.HDFS,
        root="/",
        params={
            "host": "127.0.0.1",
            "port": 54310,
            "user": "hadoop",
            "driverType": DriverType.LIBHDFS3,
            "kerberosTicket": "",
        },
    )
    print(fs_status)


def deregister_hdfs():
    fs_status = pyblazing.deregister_file_system(authority="myLocalHdfs")
    print(fs_status)


def register_posix():

    import os

    dir_path = os.path.dirname(os.path.realpath(__file__))

    print("*** Register a POSIX File System ***")
    fs_status = pyblazing.register_file_system(
        authority="mortgage", type=FileSystemType.POSIX, root=dir_path
    )
    print(fs_status)


def deregister_posix():
    fs_status = pyblazing.deregister_file_system(authority="mortgage")
    print(fs_status)


def open_perf_table(table_ref):
    for key in table_ref.keys():
        sql = "select * from main.%(table_name)s" % {"table_name":
                                                     key.table_name}
        return pyblazing.run_query(sql, table_ref)


def run_gpu_workflow(quarter=1, year=2000, perf_file="", **kwargs):

    import time

    load_start_time = time.time()
    names = createSchema.gpu_load_names(col_names_path)
    print(acq_data_path)
    acq_gdf = createSchema.gpu_load_acquisition_csv(
        acquisition_path=acq_data_path
        + "/Acquisition_"
        + str(year)
        + "Q"
        + str(quarter)
        + ".txt"
    )
    gdf = createSchema.gpu_load_performance_csv(perf_file)
    load_end_time = time.time()
    etl_start_time = time.time()

    acq_gdf_results = merge_names(acq_gdf, names)

    everdf_results = create_ever_features(gdf)

    delinq_merge_results = create_delinq_features(gdf)

    new_everdf_results = join_ever_delinq_features(
        everdf_results.columns, delinq_merge_results.columns
    )

    joined_df_results = create_joined_df(gdf.columns,
                                         new_everdf_results.columns)

    del new_everdf_results

    testdf_results = create_12_mon_features_union(joined_df_results.columns)

    testdf = testdf_results.columns
    new_joined_df_results = combine_joined_12_mon(joined_df_results.columns,
                                                  testdf)
    del testdf
    del joined_df_results
    perf_df_results = final_performance_delinquency(
        gdf.columns, new_joined_df_results.columns
    )
    del gdf
    del new_joined_df_results

    final_gdf_results = join_perf_acq_gdfs(
        perf_df_results.columns, acq_gdf_results.columns
    )
    del perf_df_results
    del acq_gdf_results

    final_gdf = last_mile_cleaning(final_gdf_results.columns)

    etl_end_time = time.time()

    return [
        final_gdf,
        (load_end_time - load_start_time),
        (etl_end_time - etl_start_time),
    ]


def merge_names(names_table, acq_table):
    chronometer = Chronometer.makeStarted()
    tables = {names_table.name: names_table.columns,
              acq_table.name: acq_table.columns}

    query = """
        SELECT loan_id, orig_channel, orig_interest_rate, orig_upb,
            orig_loan_term, orig_date, first_pay_date, orig_ltv, orig_cltv,
            num_borrowers, dti, borrower_credit_score, first_home_buyer,
            loan_purpose, property_type, num_units, occupancy_status,
            property_state, zip, mortgage_insurance_percent, product_type,
            coborrow_credit_score, mortgage_insurance_type,
            relocation_mortgage_indicator, new_seller_name as seller_name
        FROM main.acq as a
        LEFT OUTER JOIN main.names as n ON  a.seller_name = n.seller_name
    """
    result = pyblazing.run_query(query, tables)
    Chronometer.show(chronometer, "Create Acquisition (Merge Names)")
    return result


def create_ever_features(table, **kwargs):
    chronometer = Chronometer.makeStarted()
    query = """SELECT loan_id,
        max(current_loan_delinquency_status) >= 1 as ever_30,
        max(current_loan_delinquency_status) >= 3 as ever_90,
        max(current_loan_delinquency_status) >= 6 as ever_180
        FROM main.perf group by loan_id"""
    result = pyblazing.run_query(query, {table.name: table.columns})
    Chronometer.show(chronometer, "Create Ever Features")
    return result


def create_delinq_features(table, **kwargs):
    chronometer = Chronometer.makeStarted()
    query = """
        SELECT loan_id,
            min(monthly_reporting_period) as delinquency_30
        FROM main.perf
        where current_loan_delinquency_status >= 1 group by loan_id"""
    result_delinq_30 = pyblazing.run_query(query, {table.name: table.columns})

    query = """
        SELECT loan_id,
            min(monthly_reporting_period) as delinquency_90
        FROM main.perf
        where current_loan_delinquency_status >= 3 group by loan_id"""
    result_delinq_90 = pyblazing.run_query(query, {table.name: table.columns})

    query = """
        SELECT loan_id,
            min(monthly_reporting_period) as delinquency_180
        FROM main.perf
        where current_loan_delinquency_status >= 6 group by loan_id
    """
    result_delinq_180 = pyblazing.run_query(query, {table.name: table.columns})

    new_tables = {
        "delinq_30": result_delinq_30.columns,
        "delinq_90": result_delinq_90.columns,
        "delinq_180": result_delinq_180.columns,
    }
    query = """
        SELECT d30.loan_id, delinquency_30,
            COALESCE(delinquency_90, DATE '1970-01-01') as delinquency_90,
            COALESCE(delinquency_180, DATE '1970-01-01') as delinquency_180
        FROM main.delinq_30 as d30
        LEFT OUTER JOIN main.delinq_90 as d90 ON d30.loan_id = d90.loan_id
        LEFT OUTER JOIN main.delinq_180 as d180 ON d30.loan_id = d180.loan_id
    """
    result_merge = pyblazing.run_query(query, new_tables)
    Chronometer.show(chronometer, "Create deliquency features")
    return result_merge


def join_ever_delinq_features(everdf_tmp, delinq_merge, **kwargs):
    chronometer = Chronometer.makeStarted()
    tables = {"everdf": everdf_tmp, "delinq": delinq_merge}
    query = """
        SELECT everdf.loan_id as loan_id, ever_30, ever_90, ever_180,
            COALESCE(delinquency_30, DATE '1970-01-01') as delinquency_30,
            COALESCE(delinquency_90, DATE '1970-01-01') as delinquency_90,
            COALESCE(delinquency_180, DATE '1970-01-01') as delinquency_180
        FROM main.everdf as everdf LEFT OUTER JOIN main.delinq as delinq
        ON everdf.loan_id = delinq.loan_id
    """
    result_merge = pyblazing.run_query(query, tables)
    Chronometer.show(chronometer, "Create ever deliquency features")
    return result_merge


def create_joined_df(gdf, everdf, **kwargs):
    chronometer = Chronometer.makeStarted()
    tables = {"perf": gdf, "everdf": everdf}

    query = """SELECT perf.loan_id as loan_id,
                perf.monthly_reporting_period as mrp_timestamp,
                EXTRACT(MONTH FROM perf.monthly_reporting_period)
                    as timestamp_month,
                EXTRACT(YEAR FROM perf.monthly_reporting_period)
                    as timestamp_year,
                COALESCE(perf.current_loan_delinquency_status, -1)
                    as delinquency_12,
                COALESCE(perf.current_actual_upb, 999999999.9) as upb_12,
                everdf.ever_30 as ever_30,
                everdf.ever_90 as ever_90,
                everdf.ever_180 as ever_180,
                COALESCE(everdf.delinquency_30, DATE '1970-01-01')
                    as delinquency_30,
                COALESCE(everdf.delinquency_90, DATE '1970-01-01')
                    as delinquency_90,
                COALESCE(everdf.delinquency_180, DATE '1970-01-01')
                    as delinquency_180
                FROM main.perf as perf
                LEFT OUTER JOIN main.everdf as everdf
                ON perf.loan_id = everdf.loan_id"""

    results = pyblazing.run_query(query, tables)
    Chronometer.show(chronometer, "Create Joined DF")
    return results


def create_12_mon_features_union(joined_df, **kwargs):
    chronometer = Chronometer.makeStarted()
    tables = {"joined_df": joined_df}
    josh_mody_n_str = "timestamp_year * 12 + timestamp_month - 24000.0"
    query = (
        "SELECT loan_id, "
        + josh_mody_n_str
        + " as josh_mody_n, max(delinquency_12) as max_d12, min(upb_12) as "
        + "min_upb_12  FROM main.joined_df as joined_df GROUP BY loan_id, "
        + josh_mody_n_str
    )
    mastertemp = pyblazing.run_query(query, tables)

    all_temps = []
    all_tokens = []
    tables = {"joined_df": mastertemp.columns}
    n_months = 12

    for y in range(1, n_months + 1):
        josh_mody_n_str = "floor((josh_mody_n - " + str(y) + ")/12.0)"
        query = (
            "SELECT loan_id, "
            + josh_mody_n_str
            + " as josh_mody_n, max(max_d12) > 3 as max_d12_gt3, "
            + "min(min_upb_12) = 0 as min_upb_12_eq0, min(min_upb_12) "
            + "as upb_12  FROM main.joined_df as joined_df GROUP BY loan_id, "
            + josh_mody_n_str
        )

        metaToken = pyblazing.run_query_get_token(query, tables)
        all_tokens.append(metaToken)

    for metaToken in all_tokens:
        temp = pyblazing.run_query_get_results(metaToken)
        all_temps.append(temp)

    y = 1
    tables2 = {"temp1": all_temps[0].columns}
    union_query = (
        "(SELECT loan_id, max_d12_gt3 + min_upb_12_eq0 as "
        + "delinquency_12, upb_12, floor(((josh_mody_n * 12) + "
        + str(24000 + (y - 1))
        + ")/12) as timestamp_year, josh_mody_n * 0 + "
        + str(y)
        + " as timestamp_month from main.temp"
        + str(y)
        + ")"
    )
    for y in range(2, n_months + 1):
        tables2["temp" + str(y)] = all_temps[y - 1].columns
        query = (
            " UNION ALL (SELECT loan_id, max_d12_gt3 + min_upb_12_eq0 as "
            + "delinquency_12, upb_12, floor(((josh_mody_n * 12) + "
            + str(24000 + (y - 1))
            + ")/12) as timestamp_year, josh_mody_n * 0 + "
            + str(y)
            + " as timestamp_month from main.temp"
            + str(y)
            + ")"
        )
        union_query = union_query + query

    results = pyblazing.run_query(union_query, tables2)
    Chronometer.show(chronometer, "Create 12 month features once")
    return results


def combine_joined_12_mon(joined_df, testdf, **kwargs):
    chronometer = Chronometer.makeStarted()
    tables = {"joined_df": joined_df, "testdf": testdf}
    query = """SELECT j.loan_id, j.mrp_timestamp, j.timestamp_month,
                 j.timestamp_year, j.ever_30, j.ever_90, j.ever_180,
                 j.delinquency_30, j.delinquency_90, j.delinquency_180,
                t.delinquency_12, t.upb_12
                FROM main.joined_df as j LEFT OUTER JOIN main.testdf as t
                ON j.loan_id = t.loan_id and
                j.timestamp_year = t.timestamp_year
                and j.timestamp_month = t.timestamp_month"""
    results = pyblazing.run_query(query, tables)
    Chronometer.show(chronometer, "Combine joind 12 month")
    return results


def final_performance_delinquency(gdf, joined_df, **kwargs):
    chronometer = Chronometer.makeStarted()
    tables = {"gdf": gdf, "joined_df": joined_df}
    query = """
        SELECT g.loan_id, current_actual_upb, current_loan_delinquency_status,
            delinquency_12, interest_rate, loan_age, mod_flag, msa,
            non_interest_bearing_upb
        FROM main.gdf as g LEFT OUTER JOIN main.joined_df as j
        ON g.loan_id = j.loan_id
        and EXTRACT(YEAR FROM g.monthly_reporting_period) = j.timestamp_year
        and EXTRACT(MONTH FROM g.monthly_reporting_period) = j.timestamp_month
    """
    results = pyblazing.run_query(query, tables)
    Chronometer.show(chronometer, "Final performance delinquency")
    return results


def join_perf_acq_gdfs(perf, acq, **kwargs):
    chronometer = Chronometer.makeStarted()
    tables = {"perf": perf, "acq": acq}
    query = """
        SELECT p.loan_id, current_actual_upb, current_loan_delinquency_status,
            delinquency_12, interest_rate, loan_age, mod_flag, msa,
            non_interest_bearing_upb, borrower_credit_score, dti,
            first_home_buyer, loan_purpose, mortgage_insurance_percent,
            num_borrowers, num_units, occupancy_status, orig_channel,
            orig_cltv, orig_date, orig_interest_rate, orig_loan_term,
            orig_ltv, orig_upb, product_type, property_state, property_type,
            relocation_mortgage_indicator, seller_name, zip
        FROM main.perf as p
        LEFT OUTER JOIN main.acq as a ON p.loan_id = a.loan_id
    """
    results = pyblazing.run_query(query, tables)
    Chronometer.show(chronometer, "Join performance acquitistion gdfs")
    return results


def last_mile_cleaning(df, **kwargs):
    chronometer = Chronometer.makeStarted()
    for col, dtype in df.dtypes.iteritems():
        if str(dtype) == "category":
            df[col] = df[col].cat.codes
        df[col] = df[col].astype("float32")
    df["delinquency_12"] = df["delinquency_12"] > 0
    df["delinquency_12"] = df["delinquency_12"].fillna(False).astype("int32")
    for column in df.columns:
        df[column] = df[column].fillna(-1)
    Chronometer.show(chronometer, "Last mile cleaning")
    return df


use_registered_hdfs = False
use_registered_posix = True

if use_registered_hdfs:
    register_hdfs()
elif use_registered_posix:
    register_posix()

# to download data for this notebook, visit
# https://rapidsai.github.io/demos/datasets/mortgage-data
# and update the following paths accordingly

Execution.getArgs()

dir_data_file = Settings.data["TestSettings"]["dataDirectory"]

acq_data_path = ""
perf_data_path = ""
col_names_path = ""
acq_data_path = dir_data_file + "/acq"
perf_data_path = dir_data_file + "/perf"
col_names_path = dir_data_file

# --------------------------------------------------------------------------------------------------------------------------

start_year = 2000
end_year = 2000  # end_year is inclusive
start_quarter = 1
end_quarter = 1
part_count = 1  # the number of data files to train against


dxgb_gpu_params = {
    "nround": 100,
    "max_depth": 8,
    "max_leaves": 2 ** 8,
    "alpha": 0.9,
    "eta": 0.1,
    "gamma": 0.1,
    "learning_rate": 0.1,
    "subsample": 1,
    "reg_lambda": 1,
    "scale_pos_weight": 2,
    "min_child_weight": 30,
    "tree_method": "gpu_hist",
    "n_gpus": 1,
    "distributed_dask": True,
    "loss": "ls",
    "objective": "reg:linear",
    "max_features": "auto",
    "criterion": "friedman_mse",
    "grow_policy": "lossguide",
    # 'nthread', ncores[worker],  # WSM may want to set this
    "verbose": True,
}


def range1(start, end):
    return range(start, end + 1)


def use_file_type_suffix(year, quarter):
    if year == 2001 and quarter >= 2:
        return True
    return False


def getChunks(year, quarter):
    if use_file_type_suffix(year, quarter):
        return range(0, 1 + 1)
    return range(0, 0 + 1)


final_cpu_df_label = None
final_cpu_df_data = None

all_load_times = []
all_etl_times = []
all_xgb_convert_times = []

for year in range1(start_year, end_year):
    for quarter in range1(start_quarter, end_quarter):
        for chunk in getChunks(year, quarter):
            if use_file_type_suffix(year, quarter) is True:
                chunk_sufix = ("_{}".format(chunk))
            else:
                chunk_sufix = ""

            perf_file = (
                perf_data_path
                + "/Performance_"
                + str(year)
                + "Q"
                + str(quarter)
                + ".txt"
                + chunk_sufix
            )

            [gpu_df, load_time, etl_time] = run_gpu_workflow(
                quarter=quarter, year=year, perf_file=perf_file
            )
            all_load_times.append(load_time)
            all_etl_times.append(etl_time)

            xgb_convert_start_time = time.time()

            gpu_df = (
                gpu_df[["delinquency_12"]],
                gpu_df[list(gpu_df.columns.difference(["delinquency_12"]))],
            )

            cpu_df_label = gpu_df[0].to_pandas()
            cpu_df_data = gpu_df[1].to_pandas()

            del gpu_df

            if year == start_year:
                final_cpu_df_label = cpu_df_label
                final_cpu_df_data = cpu_df_data
            else:
                final_cpu_df_label = pd.concat([final_cpu_df_label,
                                               cpu_df_label])
                final_cpu_df_data = pd.concat([final_cpu_df_data,
                                              cpu_df_data])

            xgb_convert_end_time = time.time()

            all_xgb_convert_times.append(
                xgb_convert_end_time - xgb_convert_start_time)

data_train, data_test, label_train, label_test = train_test_split(
    final_cpu_df_data, final_cpu_df_label, test_size=0.20, random_state=42
)

xgdf_train = xgb.DMatrix(data_train, label_train)
xgdf_test = xgb.DMatrix(data_test, label_test)

chronometerTrain1 = Chronometer.makeStarted()
startTime = time.time()
bst = xgb.train(dxgb_gpu_params, xgdf_train,
                num_boost_round=dxgb_gpu_params["nround"])
Chronometer.show(chronometerTrain1, "Train 1")

chronometerPredict1 = Chronometer.makeStarted()
preds = bst.predict(xgdf_test)
Chronometer.show(chronometerPredict1, "Predict 1")

labels = xgdf_test.get_label()
print(
    "prediction error=%f"
    % (
        sum(1 for i in range(len(preds)) if int(preds[i] > 0.5) != labels[i])
        / float(len(preds))
    )
)

endTime = time.time()
trainPredict_time = endTime - startTime

print("TIMES SUMMARY")
print("LOAD Time: %fs" % sum(all_load_times))
print("ETL Time: %fs" % sum(all_etl_times))
print("CONVERT Time: %fs" % sum(all_xgb_convert_times))
print("TRAIN/PREDICT Time: %fs" % trainPredict_time)


if use_registered_hdfs:
    deregister_hdfs()
elif use_registered_posix:
    deregister_posix()


Chronometer.show_resume()
