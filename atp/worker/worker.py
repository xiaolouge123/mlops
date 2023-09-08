import time
import hydra
import aiohttp
import asyncio
from omegaconf import DictConfig
from loguru import logger

from atp.mq.zmq import MQUtil
from docker.docker import DockerUtil
from commons.commons import ContainerStatus, Task, WorkerLoad


class Worker:
    @hydra.main(version_base=None)
    def __init__(self, cfg: DictConfig) -> None:
        self.master_config = cfg.master
        self.docker_util = DockerUtil()
        self.mq_util = MQUtil()
        self.worker_load = WorkerLoad()
        self._register_worker()

    async def run(self):
        """
        Worker task run loop, worker will automatically pull task from mq
        and decide whether to execute the task depending on the system usage and task request quota.

        """
        logger.info(
            f"Worker is running at {time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())}"
        )
        logger.info(f"Worker Node Status: {self._load_system_usage()}")
        while True:
            task = await self._task_pull()
            if task is None:
                logger.info("No task available, waiting for 10 seconds...")
                await asyncio.sleep(10)
                continue
            else:
                logger.info(f"Task {task['task_id']} is available, start executing...")
                await self.execute_docker(task)

    def get_info(self):
        """
        Get worker node information.
        """
        self.worker_load.update()
        return {
            "worker_id": self.worker_load.worker_id,
            "status": "Available",
            "workload": dict(self.worker_load),
        }

    async def _register_worker(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url=self.master_config.endpoint + "/register_worker",
                json=self.get_info(),
            ) as resp:
                if resp.status == 200:
                    logger.info("Worker registered successfully")
                else:
                    logger.error("Worker registered failed")

    async def worker_report(self):
        async with aiohttp.ClientSession() as session:
            while True:
                async with session.post(
                    url=self.master_config.endpoint + "/worker_update",
                    json=self.get_info(),
                ) as resp:
                    if resp.status == 200:
                        logger.info("Worker report successfully")
                    else:
                        logger.error("Worker report failed")
                await asyncio.sleep(10)  # 每10秒上报一次

    async def _task_pull(self):
        return await self.mq_util.pull_task_from_queue(self.worker_load)

    async def task_report(self):
        """
        Only report the task container status to master, detail info should be checked in AIM service
        """
        async with aiohttp.ClientSession() as session:
            while True:
                current_task_container = []
                for (
                    container_name,
                    statue_queue,
                ) in self.docker_util.containers_status.items():
                    last_status = None
                    while True:
                        # 拿到最近的那个状态，直到拿空
                        try:
                            last_status = await statue_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                    current_task_container.append(
                        dict(
                            ContainerStatus(
                                self.worker_load.worker_id, container_name, last_status
                            )
                        )
                    )
                async with session.post(
                    url=self.master_config.endpoint + "/task_update",
                    json=current_task_container,
                ) as resp:
                    if resp.status == 200:
                        logger.info("Task report successfully")
                    else:
                        logger.error("Task report failed")
                await asyncio.sleep(10)  # 每10秒上报一次

    async def execute_docker(self, task: Task):
        log_file = f"/log/{task.task_id}.log"
        container = self.docker_util.run_container(
            image_name=task.image_name,
            command=task.command,
            name=task.task_id,
            ports=task.ports,
            detach=True,
        )
        log_task = asyncio.create_task(
            self.docker_util.fetch_logs(container.name, log_file)
        )
        beat_task = asyncio.create_task(self.docker_util.check_status(container.name))
        await asyncio.gather(log_task, beat_task)


async def main():
    worker = Worker()
    await asyncio.gather(worker.run(), worker.worker_report(), worker.task_report())


if __name__ == "__main__":
    asyncio.run(main())
