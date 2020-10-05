from pynvml import nvmlDeviceGetHandleByIndex, nvmlDeviceGetMemoryInfo
from Configuration import Settings as Settings


class GPU_Memory:
    def __init__(self, name, start_mem, end_mem, delta):
        self.name = name
        self.start_mem = start_mem
        self.end_mem = end_mem
        self.delta = delta


def capture_gpu_memory_usage():

    info = nvmlDeviceGetMemoryInfo(nvmlDeviceGetHandleByIndex(0))
    memory_used = (info.used / 1024) / 1024
    return memory_used


def log_memory_usage(name, start_mem, end_mem):
    test = GPU_Memory(name, start_mem, end_mem, end_mem - start_mem)
    Settings.memory_list.append(test)


def print_log_gpu_memory():
    for i in range(0, len(Settings.memory_list)):
        print(
            Settings.memory_list[i].name
            + ":"
            + "   Start Mem: "
            + str(Settings.memory_list[i].start_mem)
            + "   End Mem: "
            + str(Settings.memory_list[i].end_mem)
            + "   Diff: "
            + str(Settings.memory_list[i].delta)
        )
