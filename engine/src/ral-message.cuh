#pragma once

#include <tuple>

#include <cuda_runtime.h>

#include "Utils.cuh"
#include "ResultSetRepository.h"
#include "DataFrame.h"
#include <nvstrings/NVStrings.h>
#include <nvstrings/ipc_transfer.h>

namespace libgdf {

static std::basic_string<int8_t> ConvertIpcByteArray (nvstrings_ipc_transfer ipc_data) {
  std::basic_string<int8_t> bytes;
  bytes.resize(sizeof(nvstrings_ipc_transfer));
  memcpy((void*)bytes.data(), (int8_t*)(&ipc_data), sizeof(nvstrings_ipc_transfer));
  return bytes;
}

static std::basic_string<int8_t> BuildCudaIpcMemHandler (void *data) {
  std::basic_string<int8_t> bytes;
  if (data != nullptr) {
    cudaIpcMemHandle_t ipc_memhandle;
    CheckCudaErrors(cudaIpcGetMemHandle((cudaIpcMemHandle_t *) &ipc_memhandle, (void *) data));

    bytes.resize(sizeof(cudaIpcMemHandle_t));
    memcpy((void*)bytes.data(), (int8_t*)(&ipc_memhandle), sizeof(cudaIpcMemHandle_t));

  }
  return bytes;
}

static void* CudaIpcMemHandlerFrom (const std::basic_string<int8_t>& handler) {
  void * response = nullptr;
  std::cout << "handler-content: " <<  handler.size() <<  std::endl;
  if (handler.size() == 64) {
    cudaIpcMemHandle_t ipc_memhandle;
    memcpy((int8_t*)&ipc_memhandle, handler.data(), sizeof(ipc_memhandle));
    CheckCudaErrors(cudaIpcOpenMemHandle((void **)&response, ipc_memhandle, cudaIpcMemLazyEnablePeerAccess));
  }
  return response;
}

} //namespace libgdf
