#include "Config.h"

#include <cstring>
#include <fstream>
// #include "ExceptionHandling/BlazingThread.h"

#include <sys/stat.h>
#include <sys/sysinfo.h>

#include "Util/FileUtil.h"
#include "Util/StringUtil.h"
#include "arrow/status.h"
#include <blazingdb/io/Config/BlazingContext.h>

#include <cuda.h>
#include <cuda_runtime_api.h>
int BlazingConfig::get_number_of_sms() {
	if(number_of_sms == -1) {
		cudaDeviceProp prop;
		cudaGetDeviceProperties(&prop, 0);
		number_of_sms = prop.multiProcessorCount;
	}
	return number_of_sms;
}


static bool isFalse(const std::string & value) {
	bool result = false;

	if(value == "false" || value == "FALSE" || value == "0") {
		result = true;
	}

	return result;
}

static bool isTrue(const std::string & value) {
	bool result = false;

	if(value == "true" || value == "TRUE" || value == "1") {
		result = true;
	}

	return result;
}

static void setBooleanValue(const std::string & in, bool & out) {
	if(isFalse(in)) {
		out = false;
	} else if(isTrue(in)) {
		out = true;
	} else {
		// TODO percy we should print a warning here with the property name and the default value
	}
}

template <>
long long getNullValue<long long>() {
	return NULL_LONG;
}
template <>
double getNullValue<double>() {
	return NULL_DOUBLE;
}

template <>
float getNullValue<float>() {
	return NULL_FLOAT;
}
template <>
int getNullValue<int>() {
	return NULL_INT;
}

template <>
short getNullValue<short>() {
	return NULL_SHORT;
}
template <>
char getNullValue<char>() {
	return NULL_CHAR;
}

long long getMinPossibleValueForDataType(long long value) { return LLONG_MIN; }
long long getMaxPossibleValueForDataType(long long value) { return NULL_LONG; }

double getMinPossibleValueForDataType(double value) { return DBL_MIN; }
double getMaxPossibleValueForDataType(double value) { return NULL_DOUBLE; }

float getMinPossibleValueForDataType(float value) { return FLT_MIN; }
float getMaxPossibleValueForDataType(float value) { return NULL_FLOAT; }
int getMinPossibleValueForDataType(int value) { return INT_MIN; }
int getMaxPossibleValueForDataType(int value) { return NULL_INT; }

unsigned long long getMinPossibleValueForDataType(unsigned long long value) { return 0; }


short getMinPossibleValueForDataType(short value) { return SHRT_MIN; }

short getMaxPossibleValueForDataType(short value) { return NULL_SHORT; }

char getMinPossibleValueForDataType(char value) { return CHAR_MIN; }

char getMaxPossibleValueForDataType(char value) { return NULL_CHAR; }

size_t getSizeOfDataType(std::string dataType) {
	if(dataType == "int" || dataType == "date") {
		return sizeof(int);
	} else if(dataType == "short") {
		return sizeof(short);
	} else if(dataType == "double") {
		return sizeof(double);
	} else if(dataType == "float") {
		return sizeof(float);
	} else if(dataType == "char" || dataType == "bool") {
		return sizeof(char);
	} else {  // long long and all the other string types
		return sizeof(long long);
	}
}


BlazingConfig * BlazingConfig::instance = NULL;

BlazingConfig * BlazingConfig::getInstance() {
	if(instance == NULL) {
		instance = new BlazingConfig;
	}
	return instance;
}

BlazingConfig::BlazingConfig() {
	configFileName = "";
	tempFolder = Uri("/blazing-tmp");
	baseFolder = Uri("/var/lib/blazing/blazing");
	rFolder = Uri("/var/lib/blazing/blazing-r-connector");
	uploadFolder = Uri("/var/lib/blazing/blazing-uploads");
	bufferSize = 1024 * 1024 * 128;
	maxRowSize = 1024 * 1024 * 8;
	nodesConfigFileName = baseFolder + ("/nodes.config");
	token = "community";
	checkFilePermissions = true;
	CHUNK_DEFAULT_SIZE = 100000;
	CACHE_SIZE = 0;			 // re
	DECACHE_PERIOD = 20000;  // 20 sec
	MAX_NUM_SIMULTANEOUS_QUERIES = 20;
	SIMULTANEOUS_PROCESSING_CAPACITY = 0;  // 10000000000;
	PROCESSING_REQUIREMENT_ESTIMATION_MODIFIER = 1;
	INEQUALITY_JOIN_ESTIMATION_MODIFIER = 0.1;
	NUMBER_IMPORT_BUFFERS = 5;
	perfFolder = Uri("/opt/blazing/perf");
	VIRTUAL_NODE_MULTIPLIER = 0;
	sequentialFolders = Uri("");
	sequentialFoldersList = {};
	TRANSFORMATION_PROCESSOR_OVERHEAD = 0.3;
	// numThreadsLoadDataParsingFile = BlazingThread::hardware_concurrency();
	// numThreadsForOptimization = BlazingThread::hardware_concurrency();
	// numThreadsDecompressionLoading = BlazingThread::hardware_concurrency() / 2;
	// numThreadsDecompression= BlazingThread::hardware_concurrency() / 2;

	// numThreadsLoadParquetFiles = BlazingThread::hardware_concurrency();
	// numThreadsLoadingFromDiskCache= BlazingThread::hardware_concurrency() / 2;

	// numThreadsGetFilteredDataFromCache = -1; // -1 = default: add 1 thread for every 1M rows
	// numThreadsCompressDataAndPersist = BlazingThread::hardware_concurrency();
	// numThreadsCompressDataAndPersistForChunksort = BlazingThread::hardware_concurrency() * 4;
	// numThreadsForSkipDataGenLoad = 4;
	// numThreadsForSkipDataGenProcess = BlazingThread::hardware_concurrency()/2;
	numberOfSectionsAtATimeForSkipData = 100;
	checkQueryPermissions = true;
	diskCacheEnabled = false;
	fileRetryDelay = 10000;
	hllPrecisionFactor = 12;  // this is the value p. It can be from 4 to 18

	maxNumberOfPartitionCombinations = 100000000;
	numStreamsTransformations = 1;

	maxParquetReadBatchSize = 1000000;
	diskCacheSize = 0;
	diskCacheLocation = "";
	// std::cout<<"HARDWARE CONCURRENCY IS: "<<BlazingThread::hardware_concurrency()<<std::endl;
}

void BlazingConfig::setFolders(std::string configFileName) {
	const Uri configFileNameUri = Uri(configFileName);  // TODO percy strict true?
	std::shared_ptr<arrow::io::RandomAccessFile> file =
		BlazingContext::getInstance()->getFileSystemManager()->openReadable(configFileNameUri);

	int64_t configFileSize;
	file->GetSize(&configFileSize);

	std::string configData;
	configData.resize(configFileSize);

	bool readSuccess = FileUtilv2::readCompletely(file, configFileSize, (uint8_t *) &configData[0]);
	if(!readSuccess) {
		file->Close();
		// TODO handle read fail
	}

	for(std::string line : StringUtil::split(configData, '\n')) {
		if(line.size() == 0) {
			break;
		}
		line = StringUtil::trim(line);
		line = StringUtil::replace(line, " ", "");

		if(line[0] != '#') {  // skip line if it starts with a #. It means its a comment

			std::vector<std::string> splitConfigLine = StringUtil::split(line, ":");

			std::string key = splitConfigLine[0];
			std::string value = splitConfigLine[1];
			if(splitConfigLine.size() == 3) {  // to handle paths that are like s3://namespace/
				value += ":" + splitConfigLine[2];
			}

			if(splitConfigLine.size() >= 2) {
				if(key == "BASE_FOLDER") {
					baseFolder = Uri(value);

				} else if(key == "SEQUENTIAL_DRIVES") {
					sequentialFolders = Uri(value);

					if(sequentialFolders.isEmpty()) {
						std::vector<Uri> temp;
						this->sequentialFoldersList = temp;
					} else {
						this->sequentialFoldersList = FileUtilv2::listFolders(this->sequentialFolders);
					}
					numSequentialFolders = this->sequentialFoldersList.size();
					this->distribution = std::uniform_int_distribution<long long>(0, numSequentialFolders - 1);

				} else if(key == "SEQUENTIAL_DRIVE") {
					this->sequentialFoldersList.push_back(Uri(value));
					numSequentialFolders = this->sequentialFoldersList.size();
					this->distribution = std::uniform_int_distribution<long long>(0, numSequentialFolders - 1);

				} else if(key == "BASE_FOLDER_R") {
					rFolder = Uri(value);


				} else if(key == "DISK_CACHE_SIZE") {
					this->diskCacheSize = std::atoll(value.c_str());


				} else if(key == "DISK_CACHE_LOCATION") {
					diskCacheLocation = value;


				} else if(key == "PERF_LOGGING_FOLDER") {
					perfFolder = Uri(value);


				} else if(key == "FS_NAMESPACES_FILE") {
					fsNamespacesFile = Uri(value);


				} else if(key == "BLAZING_UPLOAD_FOLDER") {
					uploadFolder = Uri(value);

				} else if(key == "BLAZING_TOKEN") {
					token = value;

				} else if(key == "BLAZING_TEMP_FOLDER") {
					tempFolder = Uri(value);

				} else if(key == "NODES_CONFIG_FILE") {
					nodesConfigFileName = Uri(value, true);

				} else if(key == "CHECK_FILE_PERMS") {
					setBooleanValue(value, this->checkFilePermissions);
				} else if(key == "CHUNK_DEFAULT_SIZE") {
					CHUNK_DEFAULT_SIZE = std::atoll(value.c_str());
				} else if(key == "CACHE_SIZE") {
					CACHE_SIZE = std::atoll(value.c_str());
				} else if(key == "DECACHE_PERIOD") {
					DECACHE_PERIOD = std::atoi(value.c_str());
				} else if(key == "MAX_NUM_SIMULTANEOUS_QUERIES") {
					MAX_NUM_SIMULTANEOUS_QUERIES = std::atoi(value.c_str());
				} else if(key == "SIMULTANEOUS_PROCESSING_CAPACITY") {
					SIMULTANEOUS_PROCESSING_CAPACITY = std::atoll(value.c_str());
				} else if(key == "PROCESSING_REQUIREMENT_ESTIMATION_MODIFIER") {
					PROCESSING_REQUIREMENT_ESTIMATION_MODIFIER = std::atof(value.c_str());
				} else if(key == "INEQUALITY_JOIN_ESTIMATION_MODIFIER") {
					INEQUALITY_JOIN_ESTIMATION_MODIFIER = std::atof(value.c_str());
				} else if(key == "NUMBER_IMPORT_BUFFERS") {
					NUMBER_IMPORT_BUFFERS = std::atoi(value.c_str());
				} else if(key == "VIRTUAL_NODE_MULTIPLIER") {
					VIRTUAL_NODE_MULTIPLIER = std::atoi(value.c_str());
				} else if(key == "TRANSFORMATION_PROCESSOR_OVERHEAD") {
					TRANSFORMATION_PROCESSOR_OVERHEAD = std::atof(value.c_str());
				} else if(key == "NUM_THREADS_DECOMPRESSION_LOADING") {
					numThreadsDecompressionLoading = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_DECOMPRESSION") {
					numThreadsDecompression = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_GET_FILT_DATA_FROM_CACHE") {
					numThreadsGetFilteredDataFromCache = std::atoi(value.c_str());
				} else if(key == "CHECK_QUERY_PERMISSIONS") {
					setBooleanValue(value, this->checkQueryPermissions);
				} else if(key == "DISK_CACHEING_ENABLED") {
					setBooleanValue(value, this->diskCacheEnabled);
				} else if(key == "BUFFER_SIZE") {
					this->bufferSize = std::atoi(value.c_str());
				} else if(key == "MAX_ROW_SIZE") {
					this->maxRowSize = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_LOADDATAPARSINGFILE") {
					numThreadsLoadDataParsingFile = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_FOROPTIMIZATION") {
					numThreadsForOptimization = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_COMPRESSDATAANDPERSIST") {
					numThreadsCompressDataAndPersist = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_COMPRESSDATAANDPERSIST_CHUNKSORT") {
					numThreadsCompressDataAndPersistForChunksort = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_SKIPDATAGENLOAD") {
					numThreadsForSkipDataGenLoad = std::atoi(value.c_str());
				} else if(key == "NUM_THREADSSKIPDATAGENPROCESS") {
					numThreadsForSkipDataGenProcess = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_LOAD_PARQUET_FILES") {
					numThreadsLoadParquetFiles = std::atoi(value.c_str());
				} else if(key == "NUM_THREADS_LOAD_FROM_DISK_CACHE") {
					numThreadsLoadingFromDiskCache = std::atoi(value.c_str());
				} else if(key == "FILE_RETRY_DELAY") {
					fileRetryDelay = std::atoi(value.c_str());
				} else if(key == "NUM_SECTIONS_AT_A_TIME_FOR_SKIPDATA") {
					numberOfSectionsAtATimeForSkipData = std::atoi(value.c_str());
				} else if(key == "HYPERLOGLOG_PRECISION_FACTOR") {
					hllPrecisionFactor = std::atoi(value.c_str());
					if(hllPrecisionFactor < 4 || hllPrecisionFactor > 18) {
						hllPrecisionFactor = 12;
					}
				} else if(key == "MAX_NUMBER_OF_PARTITION_COMBINATIONS") {
					maxNumberOfPartitionCombinations = std::atoll(value.c_str());
				} else if(key == "MAX_PARQUET_READ_BATCH_SIZE") {
					maxParquetReadBatchSize = std::atoll(value.c_str());
				}
			}
		}
	}
	this->configFileName = configFileName;

	if(!nodesConfigFileName.isValid()) {
		nodesConfigFileName = this->baseFolder + nodesConfigFileName.toString();
	}

	if(!BlazingContext::getInstance()->getFileSystemManager()->exists(tempFolder)) {
		BlazingContext::getInstance()->getFileSystemManager()->makeDirectory(tempFolder);
	}

	if(CACHE_SIZE == 0 || SIMULTANEOUS_PROCESSING_CAPACITY == 0) {
		struct sysinfo info;
		int sysInfoReturn = sysinfo(&info);
		if(sysInfoReturn != 0) {
			std::cout << "ERROR: could not get sysinfo to get totalram" << std::endl;
		}
		unsigned long long totalRam = info.totalram;
		if(CACHE_SIZE == 0) {
			CACHE_SIZE = totalRam * 0.4;
		}
		if(SIMULTANEOUS_PROCESSING_CAPACITY == 0) {
			SIMULTANEOUS_PROCESSING_CAPACITY = totalRam * 0.6;
		}
	}

	if(CACHE_SIZE < 10000000) {  // this is for backwards compatibility. Before CACHE_SIZE was in number of chunks.
		CACHE_SIZE = CACHE_SIZE * CHUNK_DEFAULT_SIZE * 8;
	}

	file->Close();
}

Uri BlazingConfig::getRandomSequentialFolder() {
	if(this->sequentialFoldersList.size() == 1) {
		return this->sequentialFoldersList[0];
	} else {
		unsigned long long randomFolderInd = this->distribution(this->generator);
		if(randomFolderInd < numSequentialFolders) {
			return this->sequentialFoldersList[randomFolderInd];
		} else {
			randomFolderInd = distribution(generator);
			if(randomFolderInd < numSequentialFolders) {
				return this->sequentialFoldersList[randomFolderInd];
			} else {
				std::cout << "ERROR in getRandomSequentialFolder()" << std::endl;
				return this->sequentialFoldersList[0];
			}
		}
	}
}

std::string getErrorMessage(const std::string & error) { return "ERROR: " + error; }
