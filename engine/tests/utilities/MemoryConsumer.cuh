#include <vector>
#include <future>
#include <chrono>

using namespace std::chrono_literals;

class MemoryConsumer
{
public:
  MemoryConsumer();
  ~MemoryConsumer() = default;

  void setOptionsMegaBytes(const std::vector<size_t> & memory_sizes, std::chrono::milliseconds delay = 250ms);
  void setOptionsPercentage(const std::vector<float> & memory_percentages, std::chrono::milliseconds delay = 250ms);

	void run();
	void stop();

private:
	bool stopRequested();

private:
	std::promise<void> exitSignal;
	std::future<void> futureObj;

  std::vector<size_t> memory_sizes; //bytes
  std::chrono::milliseconds delay;
};
