import uuid
from typing import Optional, List
from pydantic import BaseModel, root_validator

from commons.utils import get_system_usage


class ContainerStatus(BaseModel):
    worker_id: str
    container_name: str  # just is task.task_id
    status: Optional[str]  # 可能为空，当状态队列里还没有任何值塞进去


class Task(BaseModel):
    task_id: str
    image_name: str
    command: Optional[str] = None
    ports: Optional[List[int]] = None
    cpu_request: float
    mem_request: float
    disk_request: float
    gpu_request: int
    gpu_mem_request: float


class GPUInfo(BaseModel):
    gpu_id: int
    gpu_name: str
    gpu_mem_free: float
    gpu_mem_used: float
    gpu_mem_total: float
    gpu_utilization: float
    gpu_mem_utilization: float


class WorkerLoad(BaseModel):
    worker_id: Optional[str]
    cpu: Optional[float]
    cpu_usage: Optional[float]
    mem: Optional[float]
    mem_usage: Optional[float]
    disk: Optional[float]
    disk_usage: Optional[float]
    gpu: Optional[List[GPUInfo]]

    @root_validator
    def val(cls, values):
        if not values.get("worker_id"):
            values["worker_id"] = str(uuid.uuid4())[:8]
        usage = get_system_usage()
        values["cpu"] = usage.pop("cpu_cores")
        values["cpu_usage"] = usage.pop("cpu_usage_percent")
        values["mem"] = usage.pop("total_mem_in_gb")
        values["mem_usage"] = usage.pop("used_mem_in_gb")
        values["disk"] = usage.pop("total_disk_in_gb")
        values["disk_usage"] = usage.pop("used_disk_in_gb")
        values["gpu"] = [GPUInfo(**gpu) for gpu in usage.pop("gpu")]
        return values


class Worker(BaseModel):
    worker_id: str
    status: str
    workload: WorkerLoad
    register_ts: Optional[int]
    last_available_ts: Optional[int]


if __name__ == "__main__":
    wl = WorkerLoad(worker_id="cffac8a9")
    print(wl)
