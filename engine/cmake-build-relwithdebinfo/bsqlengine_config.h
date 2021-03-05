#ifndef CONFIG_BLAZINGSQL_H
#define CONFIG_BLAZINGSQL_H

#include "bsqlengine_internal_config.h"

#define STRINGIZE(x) #x
//STRINGIZE_VALUE_OF will catch a compiler definition -DKEY=VALUE and will convert the VALUE to string
#define STRINGIZE_VALUE_OF(x) STRINGIZE(x)

#define SYSTEM_NAME std::make_pair("SYSTEM_NAME", "Linux")
#define SYSTEM std::make_pair("SYSTEM", "Linux-4.13.0-45-generic")
#define SYSTEM_PROCESSOR std::make_pair("SYSTEM_PROCESSOR", "x86_64")

#define OS_RELEASE std::make_pair("OS_RELEASE", "NAME=Ubuntu|VERSION=16.04.7 LTS (Xenial Xerus)|ID=ubuntu|ID_LIKE=debian|PRETTY_NAME=Ubuntu 16.04.7 LTS|VERSION_ID=16.04|HOME_URL=http://www.ubuntu.com/|SUPPORT_URL=http://help.ubuntu.com/|BUG_REPORT_URL=http://bugs.launchpad.net/ubuntu/|VERSION_CODENAME=xenial|UBUNTU_CODENAME=xenial")
#define LSB_RELEASE std::make_pair("LSB_RELEASE", "DISTRIB_ID=Ubuntu|DISTRIB_RELEASE=16.04|DISTRIB_CODENAME=xenial|DISTRIB_DESCRIPTION=Ubuntu 16.04.7 LTS")

#define CXX_COMPILER_ID std::make_pair("CXX_COMPILER_ID", "GNU")
#define CXX_COMPILER std::make_pair("CXX_COMPILER", "/usr/bin/c++")
#define CXX_COMPILER_VERSION std::make_pair("CXX_COMPILER_VERSION", "7.5.0")

#define CMAKE_VERSION std::make_pair("CMAKE_VERSION", "3.19.2")
#define CMAKE_GENERATOR std::make_pair("CMAKE_GENERATOR", "Unix Makefiles")

#define CUDA_COMPILER std::make_pair("CMAKE_CUDA_COMPILER", "/usr/local/cuda/bin/nvcc")
#define CUDA_FLAGS std::make_pair("CMAKE_CUDA_FLAGS", " -Xcompiler -Wno-parentheses -gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_75,code=compute_75 --expt-extended-lambda --expt-relaxed-constexpr -Werror=cross-execution-space-call -Xcompiler -Wall,-Wno-error=deprecated-declarations --default-stream=per-thread -DHT_DEFAULT_ALLOCATOR")

#define BLAZINGSQL_DESCRIPTIVE_METADATA {\
BLAZINGSQL_GIT_BRANCH, \
BLAZINGSQL_GIT_COMMIT_HASH, \
BLAZINGSQL_GIT_DESCRIBE_TAG, \
BLAZINGSQL_GIT_DESCRIBE_NUMBER, \
SYSTEM_NAME, \
SYSTEM, \
SYSTEM_PROCESSOR, \
OS_RELEASE, \
LSB_RELEASE, \
CXX_COMPILER_ID, \
CXX_COMPILER, \
CXX_COMPILER_VERSION, \
CMAKE_VERSION, \
CMAKE_GENERATOR, \
CUDA_COMPILER, \
CUDA_FLAGS, \
}

#endif // CONFIG_BLAZINGSQL_H
