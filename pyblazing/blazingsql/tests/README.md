# BlazingSQL tests
Testing Automation Framework for BlazingSQL

## Dependencies
- pyspark
- drill and excel support
- GoogleSheet support

Inside a conda environment:

```shell-script
conda install --yes -c conda-forge openjdk=8.0 maven pyspark=2.4.3 pytest
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

# Run all e2e tests and use Google Docs spreadsheet to verify if current run
# pass (based on historic data). It also save a snapshot of the spreadsheet
# in the log directory (file name e2e-gspread-cache.parquet).
./test.sh pyblazing

# Run all e2e tests and use a snapshot of the Google Docs spreadsheet in
# the log directory. In case the Google Docs spreadsheet cache file
# e2e-gspread-cache.parquet is not there then it will download and save the
# snapshot.
./test.sh -c pyblazing

# Run only the round end to end test group using the spreadsheet cache file.
./test.sh -c pyblazing e2e-tests=roundTest

# Run the round and orderby end to end test groups using the spreadsheet cache file.
./test.sh -c pyblazing e2e-tests=roundTest,orderbyTest

```

#### Custom settings
All the behaviour of the end to end test are base on environment variables. So when you wan to have more control you need to change some of the default values exporting or defining the target environment variable.

#### Testing workflow remarks
- Is highly suggested to use always -c argument so we avoid making request to Google Docs API.
- Only run without the -c option when you are about to finish your PR.
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

###HDFS testing

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
