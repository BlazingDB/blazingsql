#include <algorithm>
#include <rmm/device_buffer.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <cudf/utilities/error.hpp>
#include <cuda_runtime.h>

#include "MemoryConsumer.cuh"

MemoryConsumer::MemoryConsumer()
	: futureObj(exitSignal.get_future()),	
		memory_sizes{1024 * 1024 * 1024}, // 1GB
 		delay{250ms}
{
};

void MemoryConsumer::setOptionsMegaBytes(const std::vector<size_t> & memory_sizes, std::chrono::milliseconds delay){
	this->memory_sizes.resize(memory_sizes.size());
	std::transform(memory_sizes.cbegin(), memory_sizes.cend(), this->memory_sizes.begin(), [](auto size){ return size * 1024 * 1024; });

	this->delay = delay;
}

void MemoryConsumer::setOptionsPercentage(const std::vector<float> & memory_percentages, std::chrono::milliseconds delay){
	size_t free_size;
	size_t total_size;
	CUDA_TRY(cudaMemGetInfo(&free_size, &total_size));

	this->memory_sizes.resize(memory_percentages.size());
	std::transform(memory_percentages.cbegin(), memory_percentages.cend(), this->memory_sizes.begin(), [total_size](auto percentage){ return static_cast<size_t>(percentage * total_size); });

	this->delay = delay;
}

void MemoryConsumer::run() {
	size_t idx = 0;
	while (stopRequested() == false) {
		size_t size = memory_sizes[idx];
		rmm::device_buffer buffer(size, rmm::cuda_stream_view{});

		idx = (idx + 1) % memory_sizes.size();

		std::this_thread::sleep_for(delay);
	}
}

void MemoryConsumer::stop()	{
	exitSignal.set_value();
}

bool MemoryConsumer::stopRequested() {
	// checks if value in future object is available
	if (futureObj.wait_for(0ms) == std::future_status::timeout)
		return false;
	return true;
}
