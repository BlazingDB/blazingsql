#include <stdexcept>

#include <Python.h>

// TODO: temporal generic error handler.
// Build a custom exception hierarchy for RAL, Calcite and pyBlazing

#define RAISE_ERROR(E)                                                                                                 \
	extern PyObject * E##Error_;                                                                                       \
	void raise##E##Error() {                                                                                           \
		try {                                                                                                          \
			if(PyErr_Occurred())                                                                                       \
				;                                                                                                      \
			else                                                                                                       \
				throw;                                                                                                 \
		} catch(const std::exception & e) {                                                                            \
			std::string message = std::string{"[" #E " Error] "} + e.what();                                           \
			PyErr_SetString(E##Error_, message.c_str());                                                               \
		} catch(...) {                                                                                                 \
			PyErr_SetString(PyExc_RuntimeError, "Unknown " #E " Error");                                               \
		}                                                                                                              \
	}

RAISE_ERROR(Initialize)
RAISE_ERROR(Finalize)
RAISE_ERROR(BlazingSetAllocator)
RAISE_ERROR(GetProductDetails)
RAISE_ERROR(PerformPartition)
RAISE_ERROR(RunGenerateGraph)
RAISE_ERROR(RunExecuteGraph)
RAISE_ERROR(RunSkipData)
RAISE_ERROR(ParseSchema)
RAISE_ERROR(RegisterFileSystemHDFS)
RAISE_ERROR(RegisterFileSystemGCS)
RAISE_ERROR(RegisterFileSystemS3)
RAISE_ERROR(RegisterFileSystemLocal)
