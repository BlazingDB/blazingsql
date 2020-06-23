#include <Python.h>

PyObject *InitializeError_ = nullptr,
				 *FinalizeError_ = nullptr,
				 *PerformPartitionError_ = nullptr,
				 *RunGenerateGraphError_ = nullptr,
				 *RunExecuteGraphError_ = nullptr,
				 *RunSkipDataError_ = nullptr,
		 		 *ParseSchemaError_ = nullptr,
				 *RegisterFileSystemHDFSError_ = nullptr,
				 *RegisterFileSystemGCSError_ = nullptr,
		 		 *RegisterFileSystemS3Error_ = nullptr,
				 *RegisterFileSystemLocalError_ = nullptr,
				 *BlazingSetAllocatorError_ = nullptr,
				 *GetProductDetailsError_ = nullptr;


// PyErr_SetString
