

#ifndef FILEUTILV2_H_
#define FILEUTILV2_H_

#include <sys/stat.h>

#include <vector>
#include <dirent.h>
#include <string>
#include <mutex>
#include <iosfwd>
#include "arrow/io/interfaces.h"
#include <arrow/api.h>


class Uri;

class Path;
class FileUtilv2 {
public:


	static bool filePathContainsWildcards(std::string & filePath);

	static std::vector<Uri> getFilesWithWildcard(std::string & filePathWithWildCard);
	static std::vector<Uri> listFolders(Uri & baseFolder);

	static bool moveAndReplace(const Uri & src, const Uri & dest);
	static bool moveAndReplaceFiles(std::vector<Uri> & srcFiles, std::vector<Uri> & destFiles);

	static bool copyFile(const Uri & src, const Uri & dst);

	static bool batchRemove(std::vector<Uri> & fileList);
	static bool batchMove(std::vector<Uri> & srcFiles, std::vector<Uri> & destFiles);
	static bool removeFolderTree(Uri rootFolder);

	static bool createFoldersForTable(Uri & tableFolder, std::vector<std::string> & column_names );

	static unsigned long long getFileSize(Uri & filePath);

	static ssize_t writeToSocket(int file, char *buf, size_t nbyte);

	static ssize_t writeToSocket(int socket, std::shared_ptr<arrow::Buffer> &buffer);

	static bool readCompletely(std::shared_ptr<arrow::io::RandomAccessFile> file, int64_t bytesToRead, uint8_t* buffer);
	static bool writeCompletely(const Uri & uri, uint8_t * data, unsigned long long dataSize);



private:


};

#endif /* FILEUTILV2_H_ */
