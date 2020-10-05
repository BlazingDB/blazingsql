from Configuration import Settings as Settings
from DataBase import createSchema as createSchema
from EndToEndTests import performanceTest
from pydrill.client import PyDrill
from Runner import runTest
from Utils import Execution


def main():
    print("**init performance test**")
    Execution.getArgs()

    dir_data_file = Settings.data["TestSettings"]["dataDirectory"]

    # Create Table Drill ------------------------------------------------
    drill = PyDrill(host="localhost", port=8047)
    createSchema.init_drill_schema(drill, dir_data_file)

    jobId = 1

    if Settings.data["MysqlConnection"]["connectEnabled"]:
        from DataBase import mysqlDatabaseManager as msqldb

        jobId = msqldb.getJobId()

    for x in range(0, 10):
        performanceTest.main(drill, dir_data_file)
        runTest.save_log(job_id=jobId)


if __name__ == "__main__":
    main()
