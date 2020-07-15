# BlazingSQL tests
Testing Automation Framework for BlazingSQL

## Dependencies
- pyspark
- drill and excel support
- GoogleSheet support

Inside a conda environment:

```shell-script
conda install --yes -c conda-forge openjdk=8.0 maven pyspark=3.0.0 pytest
pip install pydrill openpyxl pymysql gitpython pynvml gspread oauth2client
```

## Basic usage

```shell-script
cd blazingsql

# help
./test.sh -h

# run all tests: I/O, communication, engine and end to end tests (e2e)
./test.sh
```

### End to End tests

#### Default settings
By default the end to end tests:
- Run in single node only (nrals: 1)
- Compare against parquet result files instead of Drill or pySpark (execution mode: gpuci)
- The log directory is the CONDA_PREFIX folder.
- Download automatically and use the testing files (the data folder and the and parquet result files) in your CONDA_PREFIX folder (see https://github.com/BlazingDB/blazingsql-testing-files)

```shell-script
cd blazingsql

# Run all e2e tests based on your current env settings.
./test.sh e2e

# Run only the round end to end test group.
./test.sh e2e tests=roundTest

# Run the round and orderby end to end test groups.
./test.sh e2e tests=roundTest,orderbyTest
```

#### Custom settings
All the behaviour of the end to end test are base on environment variables. So when you want to have more control you need to change some of the default values exporting or defining the target environment variable before run the tests.

Examples:
```shell-script
# Run all e2e tests in full mode
BLAZINGSQL_E2E_EXEC_MODE=full ./test.sh e2e

# Run all e2e tests with 2 rals/workers using LocalCUDACluster
BLAZINGSQL_E2E_N_RALS=2 ./test.sh e2e

# Run all e2e tests with 2 rals/workers using an online dask-scheduler IP:PORT
export BLAZINGSQL_E2E_N_RALS=2
BLAZINGSQL_E2E_DASK_CONNECTION="127.0.0.1:8786" ./test.sh e2e
```

Here are all the environment variables with its default values:

```shell-script
#TestSettings
export BLAZINGSQL_E2E_DATA_DIRECTORY=$CONDA_PREFIX/blazingsql-testing-files/data/
export BLAZINGSQL_E2E_LOG_DIRECTORY=$CONDA_PREFIX/
export BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY=$CONDA_PREFIX/blazingsql-testing-files/results/
export BLAZINGSQL_E2E_DATA_SIZE="100MB2Part"
export BLAZINGSQL_E2E_EXECUTION_ENV="local"
export BLAZINGSQL_E2E_DASK_CONNECTION="local" # values: "dask-scheduler-ip:port", "local"

# AWS S3 env vars
export BLAZINGSQL_E2E_AWS_S3_BUCKET_NAME=''
export BLAZINGSQL_E2E_AWS_S3_ACCESS_KEY_ID=''
export BLAZINGSQL_E2E_AWS_S3_SECRET_KEY=''

# Google Storage env vars
export BLAZINGSQL_E2E_GOOGLE_STORAGE_PROJECT_ID=''
export BLAZINGSQL_E2E_GOOGLE_STORAGE_BUCKET_NAME=''
export BLAZINGSQL_E2E_GOOGLE_STORAGE_ADC_JSON_FILE=""

#RunSettings
export BLAZINGSQL_E2E_EXEC_MODE="gpuci" # values: gpuci, full, generator
export BLAZINGSQL_E2E_N_RALS=1
export BLAZINGSQL_E2E_N_GPUS=1
export BLAZINGSQL_E2E_NETWORK_INTERFACE="lo"
export BLAZINGSQL_E2E_SAVE_LOG=false
export BLAZINGSQL_E2E_WORKSHEET="BSQL Log Results" # or "BSQL Performance Results"
export BLAZINGSQL_E2E_LOG_INFO=''
export BLAZINGSQL_E2E_COMPARE_RESULTS=true
export BLAZINGSQL_E2E_TARGET_TEST_GROUPS=""

#ComparissonTest
export BLAZINGSQL_E2E_COMPARE_BY_PERCENTAJE=false
export BLAZINGSQL_E2E_ACCEPTABLE_DIFERENCE=0.01
```

If you don't want to use the stored parquet results (BLAZINGSQL_E2E_FILE_RESULT_DIRECTORY) and you want to compare directly against Drill and Spark then you can change the execution mode variable BLAZINGSQL_E2E_EXEC_MODE from "gpuci" to "full"
Please, note that if you want to run on full mode you must have a Drill instance running.

If you want to run a test with n rals/workers where n>1 you need to change the environment variable BLAZINGSQL_E2E_N_RALS.
Also, remember that when running a distributed tests the variable BLAZINGSQL_E2E_DASK_CONNECTION must be set to either "local" or "dask-scheduler-ip:port".
- "local" it will use dask LocalCUDACluster to simulate the n rals/workers on a single GPU.
- "dask-scheduler-ip:port" is the connection information of you dask-scheduler (in case you run manually your local dask cluster)

Finally, there are sensible data that never must be public so to get the values for these variables please ask to QA & DevOps teams:
- AWS S3 connection settings: BLAZINGSQL_E2E_AWS_S3_BUCKET_NAME, BLAZINGSQL_E2E_AWS_S3_ACCESS_KEY_ID, BLAZINGSQL_E2E_AWS_S3_SECRET_KEY
- Google Storage connection settings: BLAZINGSQL_E2E_GOOGLE_STORAGE_PROJECT_ID, BLAZINGSQL_E2E_GOOGLE_STORAGE_BUCKET_NAME, BLAZINGSQL_E2E_GOOGLE_STORAGE_ADC_JSON_FILE
- Google Docs spreadsheet access: BLAZINGSQL_E2E_LOG_INFO

#### Testing workflow remarks
- For development/debugging is recommended to set you env var BLAZINGSQL_E2E_DASK_CONNECTION to your dask-scheduler IP:PORT
- Do not touch bash files, if you need a feature please talk with QA & DevOps teams.
- Only add/modify end to end tests once you have coordinated with QA team.

### Unit tests

```shell-script
cd blazingsql

# engine/ral tests
./test.sh libengine

# I/O unit tests
./test.sh io

# communication tests
cd blazingsql
./test.sh comms
```

## Advanced usage

### HDFS testing

To support the testing of queries to a HDFS filesystem whose authentication is given by Kerberos, we provide a containerized environment that starts a fully kerborized HDFS server.


#### Requirements
- We use Docker as our container engine, that can be installed following the steps contained on the URL below:

  https://docs.docker.com/v17.09/engine/installation/linux/docker-ce/ubuntu/#os-requirements

- The utility Docker Compose is used for starting up the multi-container environment composed by the HDFS container and the Kerberos container. To install via _pip_:
	```shell-script
	$ pip install docker-compose
	```

- Download and extract a compatible Hadoop distribution that contains the client libraries (tested with versions 2.7.3 and 2.7.4):
	
	https://archive.apache.org/dist/hadoop/common/

#### Running the E2E tests with the support of HDFS and Kerberos

1. Some environment variables are needed to find the right paths to the Java dependencies; you can set them loading the script below passing the root of your Hadoop distribution:
	```shell-script
	$ cd KrbHDFS
	$ source ./load_hdfs_env_vars.sh /PATH/TO/HADOOP
	```

2. By running the script below, the Docker containers of Hadoop and Kerberos will be automatically started; also, the data located on the label _dataDirectory_ from the config file _configE2ETest.json_ will be copied inside the HDFS filesystem. Finally, the E2E framework will execute the tests and will generate the summary report.
	```shell-script
	$ cd BlazingSQLTest
	$ python -m EndToEndTests.fileSystemHdfsTest configE2ETest.json
	```

#### HDFS testing without running the E2E

To run other tests beyond the E2E tests (ad hoc scripts, local Hadoop tests), you can also start the Docker + Kerberos containers following the next steps:

1. Set the environment variables required by Hadoop:
	```shell-script
	$ cd KrbHDFS
	$ source ./load_hdfs_env_vars.sh /PATH/TO/HADOOP
	```
 
2. Run the script that we provided to start the containers. You need to pass as first argument the root of your Hadoop distribution, and the path to the data that will be copied inside the HDFS instance.
	```shell-script
	$ cd KrbHDFS
	$ ./start_hdfs.sh /PATH/TO/HADOOP /PATH/TO/YOUR/DATA
	```

3. Once your tests are finished, you could stop the containers.
	```shell-script
	$ cd KrbHDFS
	$ ./stop_hdfs.sh
	```

##### Notes:
 - For now, the starting script will require your superuser credentials.
 - You must pass a valid Kerberos ticket to the filesystem register command of BlazingSQL. For your convenience, when you start the Docker instances, the script will copy a valid ticket into the path below:
	```
	./KrbHDFS/myconf/krb5cc_0
	```

#### Hive testing

We provide as well a copy of the Apache Hive software (tested with version 1.2.2) inside the HDFS container. Therefore, the steps to run the E2E tests using Hive are similar than the instructions for HDFS.

1. Set the environment variables needed by running the script below passing the root of your Hadoop distribution:
	```shell-script
	$ cd KrbHDFS
	$ source ./load_hdfs_env_vars.sh /PATH/TO/HADOOP
	```

2. Behind the scenes, an instance of Hive linked to the HDFS server will be ready to ingest your data; so the E2E framework will execute the tests and will generate the summary report.
	```shell-script
	$ cd BlazingSQLTest
	$ python -m EndToEndTests.fileSystemHiveTest configE2ETest.json
	```

#### Troubleshooting

Sometimes, for many reasons the E2E script test could raise an error. In that case, containers may be in an invalid state. Before try again, please check that there aren't any HDFS or Kerberos containers running by calling the stopping of the containers explicitly:

```shell-script
$ cd KrbHDFS
$ docker-compose down
```

## Modules
### Data Generation
- To generate TPCH Dataset, you have to use the GenerateTpchDataFiles script

### Data Directory Structure
 - In configurationFile.json  -> 
					"dataDirectory": "/path_to_dataset/100MB2Part/"
		 
	You should have two folders inside dataDirectory: tpch folder and tpcx folder
	See https://github.com/BlazingDB/blazingsql-testing-files/blob/master/data.tar.gz

### Additional modules
- CreationDatabases

## Test types
### Drill Comparison tests
* Example:

To run all end to end test:

```shell-script
$ python -m EndToEndTests.allE2ETest configurationFile.json
```

Run performance
```shell-script
$ python allE2ETest2.py configurationFile.json
```

To run a different subset of queries that cover a particular type of queries:

```shell-script
$ python -m EndToEndTests.whereClauseTest configurationFile.json
```
