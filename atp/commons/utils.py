import psutil
import GPUtil


def get_system_usage():
    # CPU
    cpu_usage = psutil.cpu_percent(interval=1)
    cpu_cores = psutil.cpu_count()

    # Memory
    memory_info = psutil.virtual_memory()
    total_memory = memory_info.total / (1024**3)  # Convert bytes to GB
    used_memory = memory_info.used / (1024**3)  # Convert bytes to GB
    memory_percentage = memory_info.percent

    # Disk
    disk_usage = psutil.disk_usage("/")
    total_disk = disk_usage.total / (1024**3)  # Convert bytes to GB
    used_disk = disk_usage.used / (1024**3)  # Convert bytes to GB
    disk_percentage = disk_usage.percent

    # GPU
    gpus = GPUtil.getGPUs()
    gpu_data = []
    for gpu in gpus:
        gpu_data.append(
            {
                "gpu_id": gpu.id,
                "gpu_name": gpu.name,
                "gpu_mem_free": gpu.memoryFree,
                "gpu_mem_used": gpu.memoryUsed,
                "gpu_mem_total": gpu.memoryTotal,
                "gpu_utilization": gpu.load,
                "gpu_mem_utilization": gpu.memoryUtil,
            }
        )

    return {
        "cpu_cores": cpu_cores,
        "cpu_usage_percent": cpu_usage,
        "total_mem_in_gb": total_memory,
        "used_mem_in_gb": used_memory,
        "mem_usage_percent": memory_percentage,
        "total_disk_in_gb": total_disk,
        "used_disk_in_gb": used_disk,
        "disk_usage_percent": disk_percentage,
        "gpu": gpu_data,
    }


if __name__ == "__main__":
    usage_data = get_system_usage()
    for key, value in usage_data.items():
        print(f"{key}: {value}")
