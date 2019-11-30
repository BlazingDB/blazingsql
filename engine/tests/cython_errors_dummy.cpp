#include <Python.h>

PyObject *InitializeError_ = nullptr, *FinalizeError_ = nullptr, *RunQueryError_ = nullptr,
		 *ParseSchemaError_ = nullptr, *RegisterFileSystemHDFSError_ = nullptr, *RegisterFileSystemGCSError_ = nullptr,
		 *RegisterFileSystemS3Error_ = nullptr, *RegisterFileSystemLocalError_ = nullptr;
