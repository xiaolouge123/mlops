import hydra
import docker
import asyncio
from loguru import logger
from omegaconf import DictConfig


class DockerUtil:
    @hydra.main(version_base=None)
    def __init__(self, cfg: DictConfig) -> None:
        self.docker_config = cfg.docker
        self.client = docker.DockerClient(base_url=self.docker_config.endpoint)
        self.containers = {}
        self.containers_status = {}

    def run_container(
        self, image_name, command=None, name=None, detach=True, ports=None
    ):
        # TODO 添加 gpu_id 的申明
        self.client.images.pull(image_name)
        container = self.client.containers.run(
            image_name, command=command, name=name, detach=detach, ports=ports
        )
        self.containers[container.name] = container
        self.containers_status[container.name] = asyncio.Queue()
        return container

    async def fetch_logs(self, container_name, log_file):
        loop = asyncio.get_event_loop()
        container = self.containers[container_name]
        logger.add(log_file, rotation="10 MB")
        with container.logs(stream=True) as stream:
            while True:
                for line in stream:
                    await loop.run_in_executor(
                        None, logger.info, line.decode("utf-8").strip()
                    )

    async def check_status(self, container_name, interval=10):
        container = self.containers[container_name]
        c_queue = self.containers_status[container_name]
        while True:
            status = container.status
            await c_queue.put(status)
            await asyncio.sleep(interval)

    def close_container(self, container_name, force=False):
        container = self.containers[container_name]
        container.stop()
        container.remove(force=force)
        self.containers.pop(container_name)
        self.containers_status.pop(container_name)
