import aiozmq
import zmq
import hydra
import json
import asyncio
from loguru import logger
from omegaconf import DictConfig, OmegaConf

from commons.commons import WorkerLoad, Task


class MQUtil:
    # _instance = None

    # @hydra.main(version_base=None)
    # def __new__(cls, cfg: DictConfig):
    #     if not isinstance(cls._instance, cls):
    #         cls._instance = super(MQUtil, cls).__new__(cls)
    #     return cls._instance

    # @hydra.main(version_base=None)
    def __init__(self, cfg: DictConfig) -> None:
        if not hasattr(self, "mq_config"):
            self.mq_config = cfg.mq
            logger.info(f"MQUtil config: {OmegaConf.to_yaml(self.mq_config)}")

    async def send_task_to_queue(self, task: Task):
        socket = await aiozmq.create_zmq_stream(zmq.PUSH, bind=self.mq_config.endpoint)
        data_bytes = json.dumps(dict(task)).encode("utf-8")
        socket.write([data_bytes])
        await socket.drain()
        socket.close()

    async def pull_task_from_queue(self, worker_load: WorkerLoad) -> Task:
        socket = await aiozmq.create_zmq_stream(
            zmq.PULL, connect=self.mq_config.endpoint
        )

        while True:
            data_bytes = await socket.read()
            data_str = data_bytes[0].decode("utf-8")
            data = json.loads(data_str)
            task = Task(**data)
            if not check_quota(task, worker_load):
                await self.send_task_to_queue(task)
                return None
            else:
                return task


def check_quota(task: Task, worker_load: WorkerLoad) -> bool:
    """
    for simple, we only check gpu and gpu_mem
    """
    idle_gpu = 0
    for gpu in worker_load.gpu:
        if gpu.gpu_mem_utilization < 0.05 and gpu.gpu_utilization < 0.01:
            # free gpu + 1
            idle_gpu += 1
    if task.gpu_request > idle_gpu:
        return False
    return True


async def main():
    cfg = DictConfig({"mq": {"endpoint": "tcp://127.0.0.1:9019"}})
    print(cfg)
    mq1 = MQUtil(cfg)
    mq2 = MQUtil(cfg)

    cnt = 0
    while True:
        task = Task(
            task_id=f"task_{cnt}",
            image_name="ubuntu:latest",
            cpu_request=0.1,
            mem_request=0.1,
            disk_request=0.1,
            gpu_request=1,
            gpu_mem_request=0.1,
            status="pending",
            worker_id="",
        )
        await mq1.send_task_to_queue(task)
        logger.info("send msg success")
        cnt += 1
        if cnt > 10:
            break
    worker_load = WorkerLoad()
    print(worker_load)
    while True:
        task = await mq2.pull_task_from_queue(worker_load)
        if task:
            print(task)


if __name__ == "__main__":
    asyncio.run(main())
