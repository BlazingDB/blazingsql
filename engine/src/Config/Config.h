#ifndef CONFIG_H_
#define CONFIG_H_

#include <float.h>
#include <iostream>
#include <limits.h>
#include <ostream>
#include <random>
#include <string>
#include <vector>

#include <blazingdb/io/FileSystem/Uri.h>

class BlazingConfig {
public:
	static BlazingConfig * getInstance();

	Uri getBaseFolder() const { return baseFolder; }

	Uri getRFolder() const { return rFolder; }

	Uri getPerfFolder() const { return perfFolder; }
	Uri getUploadFolder() const { return uploadFolder; }

	Uri getNodesConfigFileName() const { return nodesConfigFileName; }

	std::string getToken() const { return token; }

	bool getCheckFilePermissions() const { return checkFilePermissions; }

	unsigned long long getChunkDefaultSize() const { return CHUNK_DEFAULT_SIZE; }

	long long getCacheSize() const { return CACHE_SIZE; }

	int getDechacheingPeriod() const { return DECACHE_PERIOD; }

	int getMaxNumOfSimulteneousQueries() const { return MAX_NUM_SIMULTANEOUS_QUERIES; }

	unsigned long long getSimultaneousProcessingCapacity() const { return SIMULTANEOUS_PROCESSING_CAPACITY; }

	double getProcessingRequirmentsEstimationModifier() const { return PROCESSING_REQUIREMENT_ESTIMATION_MODIFIER; }

	double getInequalityJoinEstimationModifier() const { return INEQUALITY_JOIN_ESTIMATION_MODIFIER; }

	int getNumberOfImportBuffers() const { return NUMBER_IMPORT_BUFFERS; }

	int getVirtualNodeMultiplier() const { return VIRTUAL_NODE_MULTIPLIER; }
	void setVirtualNodeMultiplier(int numNodes) { VIRTUAL_NODE_MULTIPLIER = numNodes; }

	void setFolders(std::string configFileName);

	void refresh() { setFolders(this->configFileName); }

	Uri getTempFolder() const { return this->tempFolder; }

	Uri getFSNamespacesFile() const { return this->fsNamespacesFile; }

	Uri getRandomSequentialFolder();

	std::vector<Uri> getSequentialFolders() { return this->sequentialFoldersList; }

	Uri getSequentialFoldersRoot() { return this->sequentialFolders; }

	double transformationProcessorOverhead() const { return TRANSFORMATION_PROCESSOR_OVERHEAD; }

	short getNumThreadsLoadDataParsingFile() const { return numThreadsLoadDataParsingFile; }

	short getNumThreadsForOptimization() const { return numThreadsForOptimization; }

	short getNumThreadsDecompressionLoading() const { return numThreadsDecompressionLoading; }

	short getNumThreadsDecompression() const { return numThreadsDecompression; }

	short getNumThreadsGetFilteredDataFromCache() const { return numThreadsGetFilteredDataFromCache; }

	short getNumThreadsCompressDataAndPersisit() const { return numThreadsCompressDataAndPersist; }

	void setNumThreadsCompressDataAndPersisit(short numThreads) { numThreadsCompressDataAndPersist = numThreads; }

	short getNumThreadsCompressDataAndPersisitForChunksort() const {
		return numThreadsCompressDataAndPersistForChunksort;
	}

	short getNumThreadsForSkipDataGenLoad() const { return numThreadsForSkipDataGenLoad; }

	short getNumThreadsForSkipDataGenProcess() const { return numThreadsForSkipDataGenProcess; }

	size_t getBufferSize() const { return this->bufferSize; }

	unsigned long long getMaxRowSize() const { return this->maxRowSize; }

	bool getCheckQueryPermissions() const { return checkQueryPermissions; }

	bool getDiskCacheEnabled() const { return diskCacheEnabled; }

	int getFileRetryDelay() const { return fileRetryDelay; }

	int getNumberOfSectionsAtATimeForSkipData() const { return numberOfSectionsAtATimeForSkipData; }

	unsigned long long getMaxNumberOfPartitionCombinations() const { return maxNumberOfPartitionCombinations; }

	short getHllPrecisionFactor() const { return hllPrecisionFactor; }

	short getNumThreadsLoadParquetFiles() const { return numThreadsLoadParquetFiles; }

	short getNumThreadsLoadingFromDiskCache() const { return numThreadsLoadingFromDiskCache; }

	int get_number_of_sms();
	unsigned long long getMaxParquetReadBatchSize() const { return maxParquetReadBatchSize; }

	int getNumStreamsTransformations() const { return numStreamsTransformations; }
	unsigned long long getDiskCacheSize() { return this->diskCacheSize; }

	Uri getDiskCacheLocation() { return this->diskCacheLocation; }

private:
	BlazingConfig();
	std::string configFileName;
	Uri baseFolder;
	Uri rFolder;
	unsigned long long maxRowSize;
	size_t bufferSize;
	Uri uploadFolder;
	std::string token;
	Uri sequentialFolders;
	std::vector<Uri> sequentialFoldersList;
	Uri tempFolder;
	Uri perfFolder;
	Uri fsNamespacesFile;
	static BlazingConfig * instance;
	Uri nodesConfigFileName;  // file will be located in the baseFolder
	unsigned long long CHUNK_DEFAULT_SIZE;
	long long CACHE_SIZE;
	int DECACHE_PERIOD;  // in milliseconds
	int MAX_NUM_SIMULTANEOUS_QUERIES;
	unsigned long long
		SIMULTANEOUS_PROCESSING_CAPACITY;  // in bytes. How much operating memory it has to process queries
										   // simultaneously. Currently based entirely off of heuristic estimates
	double PROCESSING_REQUIREMENT_ESTIMATION_MODIFIER;  // modifier for query memory processing requirements
	double INEQUALITY_JOIN_ESTIMATION_MODIFIER;			// modifier for query memory processing requirements
	unsigned long long maxNumberOfPartitionCombinations;
	bool checkFilePermissions;
	int NUMBER_IMPORT_BUFFERS;
	int VIRTUAL_NODE_MULTIPLIER;
	std::default_random_engine generator;
	std::uniform_int_distribution<long long> distribution;
	int numSequentialFolders = 0;

	double TRANSFORMATION_PROCESSOR_OVERHEAD;  // Range [0,1] this much memory overhead will use the transformation
											   // processor by maxBatchsize
	short numThreadsLoadDataParsingFile;
	short numThreadsForOptimization;
	short numThreadsDecompressionLoading;  // default hardware_concurrency / 2
	short numThreadsDecompression;		   // default hardware_concurrency / 2

	short numThreadsLoadingFromDiskCache;

	short numThreadsLoadParquetFiles;

	short numThreadsGetFilteredDataFromCache = -1;  // -1 = default: add 1 thread for every 1M rows
	short numThreadsCompressDataAndPersist;
	short numThreadsCompressDataAndPersistForChunksort;
	short numThreadsForSkipDataGenLoad;
	short numThreadsForSkipDataGenProcess;
	int numberOfSectionsAtATimeForSkipData;
	int fileRetryDelay;
	int numStreamsTransformations;

	bool checkQueryPermissions = true;

	bool diskCacheEnabled;

	short hllPrecisionFactor;  // this is the value p. It can be from 4 to 18

	Uri diskCacheLocation;
	unsigned long long diskCacheSize;

	unsigned long long maxParquetReadBatchSize;
	int number_of_sms = -1;
};

typedef union {
	long long num;
	char text[8];
} string_long;

typedef union {
	long long num;
	double doubleNum;
} double_long;

enum BufferStatus { Available, Loading, Loaded, Processing };

struct ProcessingQueueElement {
	BufferStatus status;
	int64_t startInd;
	int64_t endInd;
};

static std::string STRING_LABEL = "string";  // standard string data type label (without hash requirements designation)
static std::string STRING_LABEL_wHASH = "string_with_hash";		// string data with hash
static std::string STRING_LABEL_woHASH = "string_wo_hash";		// string data without hash
static std::string STRING_LABEL_HASHONLY = "string_hash_only";  // string data hash only
static std::string STRING_LABEL_CANTHASH = "string_cant_hash";  // string data not hash, cant hash
static std::string LONG_LABEL = "long";

static int NO_COLUMN_FOUND_EXCEPTION = 11110001;

// null values
static long long NULL_ROW = -1;
static long long NULL_LONG = (LLONG_MAX - 1) / 2 + 1;
static int NULL_INT =
	(INT_MAX - 1) / 2 + 1;  // 1073741824;   // our valid range is -1073741823 to 1073741823 which is half of what c++
							// allows, but we need to be able to do delta compression on it
static short NULL_SHORT = SHRT_MAX;
static char NULL_CHAR = CHAR_MAX;
static double NULL_DOUBLE = 1.7976931348623123e+308;
static float NULL_FLOAT = FLT_MAX;

static std::string NULL_DATE = "92188684";

// these are the possible states for Cache::loaded
static const char NOT_LOADED = 0;
static const char LOAD_STARTED = 1;
static const char LOADED_FROM_DISK = 2;
static const char DECOMPRESS_STARTED = 3;
static const char LOADED = 4;

template <class T>
T getNullValue() {
	return 0;
}

// remember to declare specialization in header or strange linking errors may occur
template <>
long long getNullValue<long long>();
template <>
double getNullValue<double>();
template <>
float getNullValue<float>();
template <>
int getNullValue<int>();
template <>
short getNullValue<short>();
template <>
char getNullValue<char>();

unsigned long long getMinPossibleValueForDataType(unsigned long long value);

long long getMinPossibleValueForDataType(long long value);

double getMinPossibleValueForDataType(double value);

long long getMaxPossibleValueForDataType(long long value);

double getMaxPossibleValueForDataType(double value);

float getMinPossibleValueForDataTypefl(float value);

int getMinPossibleValueForDataType(int value);

float getMaxPossibleValueForDataType(float value);

int getMaxPossibleValueForDataType(int value);

short getMinPossibleValueForDataType(short value);

char getMinPossibleValueForDataType(char value);

short getMaxPossibleValueForDataType(short value);

char getMaxPossibleValueForDataType(char value);

size_t getSizeOfDataType(std::string dataType);

std::string getErrorMessage(const std::string & error);


typedef int partitionNumberType;
typedef unsigned int rowInPartitionType;

struct activeTableIndex {
	partitionNumberType partitionInd;
	rowInPartitionType rowInd;

	activeTableIndex() {}
	activeTableIndex(partitionNumberType partitionIndIn, rowInPartitionType rowIndIn) {
		partitionInd = partitionIndIn;
		rowInd = rowIndIn;
	}

	activeTableIndex & operator=(const activeTableIndex & other) {
		partitionInd = other.partitionInd;
		rowInd = other.rowInd;
		return *this;
	}

	bool operator==(const activeTableIndex & rhs) const {
		return partitionInd == rhs.partitionInd && rowInd == rhs.rowInd;
	}

	bool operator!=(const activeTableIndex & rhs) const {
		return partitionInd != rhs.partitionInd || rowInd != rhs.rowInd;
	}

	bool operator<(const activeTableIndex & rhs) const {
		return partitionInd < rhs.partitionInd || (partitionInd == rhs.partitionInd && (rowInd < rhs.rowInd));
	}

	bool operator>(const activeTableIndex & rhs) const {
		return partitionInd > rhs.partitionInd || (partitionInd == rhs.partitionInd && (rowInd > rhs.rowInd));
	}

	void swap(activeTableIndex & other) {
		std::swap(partitionInd, other.partitionInd);
		std::swap(rowInd, other.rowInd);
	}

	bool isNull() { return partitionInd < 0; }
};

static activeTableIndex NULL_ACTIVETABLEINDEX(-1, 0);


#endif
