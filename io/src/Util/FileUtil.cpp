#include "FileUtil.h"

#include "ExceptionHandling/BlazingThread.h"
#include <cerrno>
#include <chrono>
#include <fstream>
#include <iostream>
#include <string.h>

#include <dirent.h>
#include <fcntl.h>  // O_RDONLY
#include <libgen.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>  // read
#include <unistd.h>

#include "FileUtil.h"


#include "FileSystem/Path.h"
#include "FileSystem/Uri.h"
#include "Util/StringUtil.h"

#include "Config/BlazingContext.h"
#include "FileSystem/FileSystemInterface.h"

#include "Library/Logging/Logger.h"
namespace Logging = Library::Logging;

static const unsigned int SLEEP_BASE_TIME = 16;
const int FILE_RETRY_DELAY = 1000;

bool FileUtilv2::filePathContainsWildcards(std::string & filePath) {
	std::string wildOption1 = "*";
	std::string wildOption2 = "?";
	return StringUtil::contains(filePath, wildOption1) || StringUtil::contains(filePath, wildOption2);
}


std::vector<Uri> FileUtilv2::getFilesWithWildcard(std::string & filePathWithWildCard) {
	Path pathWithWildCard = Path(filePathWithWildCard, false);

	std::string pattern = pathWithWildCard.getResourceName();

	Path path = pathWithWildCard.getParentPath();
	std::string pathStr = path.toString();

	if(FileUtilv2::filePathContainsWildcards(pathStr)) {
		std::vector<Uri> folders = FileUtilv2::getFilesWithWildcard(pathStr);

		std::vector<Uri> urisToReturn;
		for(Uri folder : folders) {
			std::vector<Uri> urisFound = BlazingContext::getInstance()->getFileSystemManager()->list(folder, pattern);
			urisToReturn.insert(urisToReturn.end(), urisFound.begin(), urisFound.end());
		}
		return urisToReturn;
	} else {
		Uri folder = Uri(path.toString());
		return BlazingContext::getInstance()->getFileSystemManager()->list(folder, pattern);
	}
}

std::vector<Uri> FileUtilv2::listFolders(Uri & baseFolder) {
	std::vector<FileStatus> fileStatusList =
		BlazingContext::getInstance()->getFileSystemManager()->list(baseFolder, DirsFilter());

	std::vector<Uri> uriList;
	for(FileStatus status : fileStatusList) {
		if(status.isDirectory()) {
			uriList.push_back(status.getUri());
		}
	}
	return uriList;
}


bool FileUtilv2::moveAndReplace(const Uri & src, const Uri & dest) {
	bool success = true;
	if(BlazingContext::getInstance()->getFileSystemManager()->exists(src)) {
		if(BlazingContext::getInstance()->getFileSystemManager()->exists(dest)) {
			success = success && BlazingContext::getInstance()->getFileSystemManager()->remove(dest);
		}
		success = success && BlazingContext::getInstance()->getFileSystemManager()->move(src, dest);
	}
	return success;
}

void moveAndReplaceFilesThread(long long & fileInd,
	std::mutex & completionMutex,
	std::vector<Uri> & srcFiles,
	std::vector<Uri> & destFiles,
	int & successCount,
	int & errorCount) {
	int finalFileInd = srcFiles.size() - 1;
	completionMutex.lock();
	int curFile = fileInd;
	fileInd++;
	completionMutex.unlock();
	while(curFile <= finalFileInd) {
		bool tempSuccess = false;
		try {
			tempSuccess = FileUtilv2::moveAndReplace(srcFiles[curFile], destFiles[curFile]);
		} catch(...) {
			tempSuccess = false;
			completionMutex.lock();
			errorCount++;
			completionMutex.unlock();
		}

		completionMutex.lock();
		curFile = fileInd;
		fileInd++;
		if(tempSuccess) {
			successCount++;
		}
		completionMutex.unlock();
	}
}


bool FileUtilv2::moveAndReplaceFiles(std::vector<Uri> & srcFiles, std::vector<Uri> & destFiles) {
	size_t numThreads = BlazingThread::hardware_concurrency();
	numThreads = numThreads < srcFiles.size() ? numThreads : srcFiles.size();
    size_t maxThreads = 8;
	numThreads = numThreads < maxThreads ? numThreads : maxThreads;
	std::vector<BlazingThread> filesThread(numThreads);

	int successCount = 0;
	int errorCount = 0;

	std::mutex completionMutex;
	long long fileInd = 0;

#ifdef SINGLE_THREADED
	moveAndReplaceFilesThread(fileInd, completionMutex, srcFiles, destFiles, successCount, errorCount);
#else
	for(size_t j = 0; j < numThreads; ++j) {
		filesThread[j] = BlazingThread(&moveAndReplaceFilesThread,
			std::ref(fileInd),
			std::ref(completionMutex),
			std::ref(srcFiles),
			std::ref(destFiles),
			std::ref(successCount),
			std::ref(errorCount));
	}
	for(size_t j = 0; j < numThreads; ++j) {
		filesThread[j].join();
	}
#endif

	if(errorCount > 0) {
		Library::Logging::Logger().logError("Caught " + std::to_string(errorCount) +
											" in moveAndReplaceFiles when moving " + std::to_string(srcFiles.size()) +
											" files");
	}

	return static_cast<size_t>(successCount) == srcFiles.size();
}


void _createTableFolderCreationThread(std::vector<Uri> & sequentialDrives,
	Uri & baseFolder,
	int & driveInd,
	std::mutex & completionMutex,
	Uri & tableFolder,
	std::vector<std::string> & colNames,
	bool & success) {
	int lastDrive = sequentialDrives.size() - 1;
	completionMutex.lock();
	int curDrive = driveInd;
	driveInd++;
	completionMutex.unlock();

	while(curDrive <= lastDrive) {
		const Uri sequentialDriveUri1 = tableFolder.replaceParentUri(baseFolder, sequentialDrives[curDrive]);

		const bool ok = BlazingContext::getInstance()->getFileSystemManager()->makeDirectory(sequentialDriveUri1);

		if(ok == false) {
			success = false;
			return;
		}

		for(auto & colName : colNames) {
			Uri colFolder = tableFolder + ("/" + colName);

			const Uri sequentialDriveUri2 = colFolder.replaceParentUri(baseFolder, sequentialDrives[curDrive]);

			const bool ok = BlazingContext::getInstance()->getFileSystemManager()->makeDirectory(sequentialDriveUri2);

			if(ok == false) {
				success = false;
				return;
			}
		}

		completionMutex.lock();
		curDrive = driveInd;
		driveInd++;
		completionMutex.unlock();
	}

	success = true;
}

bool FileUtilv2::createFoldersForTable(Uri & /*tableFolder*/, std::vector<std::string> & /*column_names*/) {
	/*
		std::vector<Uri> sequentialDrives = BlazingConfig::getInstance()->getSequentialFolders();
		Uri baseFolder = BlazingConfig::getInstance()->getBaseFolder();
		sequentialDrives.push_back(baseFolder); // here we are including the baseFolder so that we make folders in there
	   too

		int numThreads = BlazingThread::hardware_concurrency() * 2;
		numThreads = sequentialDrives.size() < numThreads ? sequentialDrives.size() : numThreads;
		int driveInd = 0;
		bool success = true;
		std::mutex completionMutex;
		std::vector<BlazingThread> mkdirThreads(numThreads);
		for (int i = 0; i < numThreads; i++){
			mkdirThreads[i] = BlazingThread(&_createTableFolderCreationThread, std::ref(sequentialDrives),
	   std::ref(baseFolder), std::ref(driveInd), std::ref(completionMutex), std::ref(tableFolder),
	   std::ref(column_names), std::ref(success));
		}
		for (int i = 0; i < numThreads; i++){
			mkdirThreads[i].join();
		}*/
	return true;
}

void _removeThread(long long & fileInd,
	std::mutex & completionMutex,
	std::vector<Uri> & fileList,
	int & successCount,
	int & errorCount) {
	int finalFileInd = fileList.size() - 1;
	completionMutex.lock();
	int curFile = fileInd;
	fileInd++;
	completionMutex.unlock();
	while(curFile <= finalFileInd) {
		bool tempSuccess = false;
		try {
			tempSuccess = BlazingContext::getInstance()->getFileSystemManager()->remove(fileList[curFile]);
		} catch(...) {
			tempSuccess = false;
			completionMutex.lock();
			errorCount++;
			completionMutex.unlock();
		}

		completionMutex.lock();
		curFile = fileInd;
		fileInd++;
		if(tempSuccess) {
			successCount++;
		}
		completionMutex.unlock();
	}
}


bool FileUtilv2::batchRemove(std::vector<Uri> & fileList) {
    size_t numThreads = BlazingThread::hardware_concurrency();
	numThreads = numThreads < (fileList.size() + 9) / 10 ? numThreads : (fileList.size() + 9) / 10;
    size_t maxThreads = 8;
	numThreads = numThreads < maxThreads ? numThreads : maxThreads;
	std::vector<BlazingThread> filesThread(numThreads);

	int successCount = 0;
	int errorCount = 0;

	std::mutex completionMutex;
	long long fileInd = 0;

#ifdef SINGLE_THREADED
	_removeThread(fileInd, completionMutex, fileList, successCount, errorCount);
#else
	for(size_t j = 0; j < numThreads; ++j) {
		filesThread[j] = BlazingThread(&_removeThread,
			std::ref(fileInd),
			std::ref(completionMutex),
			std::ref(fileList),
			std::ref(successCount),
			std::ref(errorCount));
	}
	for(size_t j = 0; j < numThreads; ++j) {
		filesThread[j].join();
	}
#endif

	if(errorCount > 0) {
		Library::Logging::Logger().logError("Caught " + std::to_string(errorCount) +
											" in batchRemove() when removing " + std::to_string(fileList.size()) +
											" files");
	}

	return static_cast<size_t>(successCount) == fileList.size();
}


bool FileUtilv2::removeFolderTree(Uri rootFolder) {
	bool success = true;

	if(BlazingContext::getInstance()->getFileSystemManager()->exists(rootFolder)) {
		FileOrFolderFilter filter;
		std::vector<FileStatus> list = BlazingContext::getInstance()->getFileSystemManager()->list(rootFolder, filter);

		std::vector<Uri> toRemove;

		for(FileStatus status : list) {
			if(status.isDirectory()) {
				success = success && FileUtilv2::removeFolderTree(status.getUri());
			}
			toRemove.push_back(status.getUri());
		}

		success = success && FileUtilv2::batchRemove(toRemove);
		success = success && BlazingContext::getInstance()->getFileSystemManager()->remove(rootFolder);
	}
	return success;
}


unsigned long long FileUtilv2::getFileSize(Uri & filePath) {
	FileStatus status = BlazingContext::getInstance()->getFileSystemManager()->getFileStatus(filePath);

	return status.getFileSize();
}
ssize_t FileUtilv2::writeToSocket(int socket, std::shared_ptr<arrow::Buffer> & buffer) {
	return FileUtilv2::writeToSocket(
		socket, static_cast<char *>((void *) (buffer->data())), static_cast<std::size_t>(buffer->size()));
}

ssize_t FileUtilv2::writeToSocket(int socket, char * buf, size_t nbyte) {
	size_t fail = -1;
	size_t amountWrote = 0;
	size_t bytesWrote = write(socket, buf, nbyte);
	int countInvalids = 0;

	if(bytesWrote != fail) {
		amountWrote = bytesWrote;
	} else {
		const std::string sysError(std::strerror(errno));
		Library::Logging::Logger().logError(
			"FileUtilv2::writeToSocket fails writing to a socket on first write. Error was: " + sysError + " (" +
			std::to_string(errno) + ")");
	}

	while(amountWrote < nbyte) {
		bytesWrote = write(socket, buf + amountWrote, nbyte - amountWrote);

		if(bytesWrote != fail) {
			amountWrote += bytesWrote;
			countInvalids = 0;
		} else {
			if(errno == 9) {  // Bad socket number
				const std::string sysError(std::strerror(errno));
				Library::Logging::Logger().logError("FileUtilv2::writeToSocket fails writing to a socket. Error was: " +
													sysError + " (" + std::to_string(errno) + ")");
				return fail;
			}

			int fileRetryDelay = FILE_RETRY_DELAY;
			const int sleep_milliseconds = (countInvalids + 1) * fileRetryDelay;
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_milliseconds));
			if(countInvalids < 300) {
				countInvalids++;
			}

			if(countInvalids % 5 == 0) {
				const std::string sysError(std::strerror(errno));
				Library::Logging::Logger().logError("FileUtilv2::writeToSocket fails writing to a socket. Error was: " +
													sysError + " (" + std::to_string(errno) + ")");
			}
		}
	}
	return amountWrote;
}

void _moveThread(long long & fileInd,
	std::mutex & completionMutex,
	std::vector<Uri> & srcFiles,
	std::vector<Uri> & destFiles,
	int & successCount,
	int & errorCount) {
	int finalFileInd = srcFiles.size() - 1;
	completionMutex.lock();
	int curFile = fileInd;
	fileInd++;
	completionMutex.unlock();
	while(curFile <= finalFileInd) {
		bool tempSuccess = false;
		try {
			tempSuccess =
				BlazingContext::getInstance()->getFileSystemManager()->move(srcFiles[curFile], destFiles[curFile]);
		} catch(...) {
			tempSuccess = false;
			completionMutex.lock();
			errorCount++;
			completionMutex.unlock();
		}

		completionMutex.lock();
		curFile = fileInd;
		fileInd++;
		if(tempSuccess) {
			successCount++;
		}
		completionMutex.unlock();
	}
}


bool FileUtilv2::batchMove(std::vector<Uri> & srcFiles, std::vector<Uri> & destFiles) {
	if(srcFiles.size() != destFiles.size()) {
		Library::Logging::Logger().logError("FileUtilv2::batchMove source and destination set are not the same size");
		return false;
	}

    size_t numThreads = BlazingThread::hardware_concurrency();
	numThreads = numThreads < (srcFiles.size() + 9) / 10 ? numThreads : (srcFiles.size() + 9) / 10;
    size_t maxThreads = 8;
	numThreads = numThreads < maxThreads ? numThreads : maxThreads;
	std::vector<BlazingThread> filesThread(numThreads);

	int successCount = 0;
	int errorCount = 0;

	std::mutex completionMutex;
	long long fileInd = 0;

#ifdef SINGLE_THREADED
	_moveThread(fileInd, completionMutex, srcFiles, destFiles, successCount, errorCount);
#else
	for(size_t j = 0; j < numThreads; ++j) {
		filesThread[j] = BlazingThread(&_moveThread,
			std::ref(fileInd),
			std::ref(completionMutex),
			std::ref(srcFiles),
			std::ref(destFiles),
			std::ref(successCount),
			std::ref(errorCount));
	}
	for(size_t j = 0; j < numThreads; ++j) {
		filesThread[j].join();
	}
#endif
	if(errorCount > 0) {
		Library::Logging::Logger().logError("Caught " + std::to_string(errorCount) + " in batchMove() when moving " +
											std::to_string(srcFiles.size()) + " files");
	}

	return static_cast<size_t>(successCount) == srcFiles.size();
}

bool FileUtilv2::copyFile(const Uri & src, const Uri & dst) {
	try {
		std::shared_ptr<arrow::io::RandomAccessFile> file =
			BlazingContext::getInstance()->getFileSystemManager()->openReadable(src);
		int64_t fileSize = file->GetSize().ValueOrDie();

		uint8_t * origData = new uint8_t[fileSize];
		bool success = FileUtilv2::readCompletely(file, fileSize, origData);
		if(!success) {
			file->Close();
			delete[] origData;
			return success;
		}
		success = FileUtilv2::writeCompletely(dst, origData, fileSize);
		delete[] origData;
		if(!success) {
			return success;
		}

	} catch(BlazingException & ex) {
		ex.append("Caught error in FileUtilv2::copyFile");
		throw;
	}
}


bool FileUtilv2::readCompletely(
	std::shared_ptr<arrow::io::RandomAccessFile> file, int64_t bytesToRead, uint8_t * buffer) {
	if(bytesToRead > 0) {
		auto status = file->Read(bytesToRead, buffer);

		if(!status.ok()) {
			Library::Logging::Logger().logError("In FileUtilv2::readCompletely on first read: " + status.status().ToString());
			return false;
		}

        int64_t totalRead = status.ValueOrDie();
        
		if(totalRead < bytesToRead) {
			int totalReadTries = 0;
			int emptyReads = 0;

			while(totalRead < bytesToRead && totalReadTries < 100 && emptyReads < 10) {
				int64_t bytesRead;
				status = file->Read(bytesToRead - totalRead, buffer + totalRead);
				if(!status.ok()) {
					Library::Logging::Logger().logError("In FileUtilv2::readCompletely on read " +
														std::to_string(totalRead) + "  status : " + status.status().ToString());
					return false;
				}
                
                bytesRead = status.ValueOrDie();
                
				if(bytesRead == 0) {
					emptyReads++;
				}
				totalRead += bytesRead;
			}
			if(totalRead < bytesToRead) {
				Library::Logging::Logger().logError(
					"In FileUtilv2::readCompletely could not read. Tried too many times");
				return false;
			} else {
				return true;
			}
		} else {
			return true;
		}
	} else {
		return true;
	}
}

bool FileUtilv2::writeCompletely(const Uri & uri, uint8_t * data, unsigned long long dataSize) {
	int count = 0;
	int errorCount = 0;
	arrow::Status status;
	if(dataSize > 0) {
		while(count < 10) {
			try {
				std::shared_ptr<arrow::io::OutputStream> outputStream =
					BlazingContext::getInstance()->getFileSystemManager()->openWriteable(uri);

				if(outputStream) {
					status = outputStream->Write((uint8_t *) data, dataSize);

					if(status.ok()) {
						outputStream->Flush();
						status = outputStream->Close();

						if(status.ok()) {
							if(errorCount > 0) {
								Logging::Logger().logWarn(
									"Caught " + std::to_string(errorCount) +
									" errors when trying to write. But did write completely in the end");
							} else if(count > 0) {
								Logging::Logger().logWarn(
									"Write failed " + std::to_string(count) +
									" times when trying to write. But did write completely in the end");
							}
							return true;
						}
					}
				}


			} catch(std::exception & e) {
				if(count > 8 || errorCount > 8) {
					throw e;
				}
				errorCount++;
			}

			int fileRetryDelay = FILE_RETRY_DELAY;
			const int sleep_milliseconds = (count + 1) * fileRetryDelay;
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_milliseconds));
			count++;
		}
	}

	if(errorCount > 0) {
		Library::Logging::Logger().logError(
			"Caught " + std::to_string(errorCount) + " errors when trying to write. Was unable to write.");
	} else if(count > 0) {
		Library::Logging::Logger().logError(
			"Write failed " + std::to_string(count) + " times when trying to write. Was unable to write.");
	}
	if(!status.ok()) {
		Logging::Logger().logTrace(status.ToString());
	}
	return false;
}
