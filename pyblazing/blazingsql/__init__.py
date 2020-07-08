from pyblazing.apiv2 import S3EncryptionType
from pyblazing.apiv2 import DataType
from pyblazing.apiv2.context import BlazingContext

from cio import getProductDetailsCaller


__version__ = getProductDetailsCaller()["BLAZINGSQL_GIT_COMMIT_HASH"]
__branch_name__ = getProductDetailsCaller()["BLAZINGSQL_GIT_BRANCH"]
__branch_tag__ = getProductDetailsCaller()["BLAZINGSQL_GIT_DESCRIBE_TAG"]
__build_id__ = getProductDetailsCaller()["BLAZINGSQL_GIT_DESCRIBE_NUMBER"]

cxx_comp_id = getProductDetailsCaller()["CXX_COMPILER_ID"]
cxx_comp = getProductDetailsCaller()["CXX_COMPILER"]
cxx_comp_version = getProductDetailsCaller()["CXX_COMPILER_VERSION"]
__compiler_version__ = cxx_comp_id + " " + cxx_comp + " " + cxx_comp_version

__cuda_flags__ = getProductDetailsCaller()["CMAKE_CUDA_FLAGS"]
__os_kernel__ = getProductDetailsCaller()["SYSTEM"]
__os_arch__ = getProductDetailsCaller()["SYSTEM_PROCESSOR"]
__os_release__ = getProductDetailsCaller()["OS_RELEASE"]


def __info__():
    print("BlazingSQL version (git hash): %s" % __version__)
    print("BlazingSQL branch name: %s" % __branch_name__)
    print("BlazingSQL branch tag: %s" % __branch_tag__)
    print("BlazingSQL build id: %s" % __build_id__)
    print("BlazingSQL compiler version: %s" % __compiler_version__)
    print("BlazingSQL cuda flags: %s" % __cuda_flags__)
    print("BlazingSQL Operating system kernel: %s" % __os_kernel__)
    print("BlazingSQL Operating system architecture: %s" % __os_arch__)
    print("BlazingSQL Linux Operating system release: %s" % __os_release__,
          flush=True)
