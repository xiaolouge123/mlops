import time
from fastapi import FastAPI
from typing import List
import threading
from loguru import logger

from commons.commons import Task, Worker, ContainerStatus
from atp.mq.zmq import MQUtil
from db.sqlite import SQLiteUtil

app = FastAPI()
mq = MQUtil()
db = SQLiteUtil()

WORKERS = {}  # {worker_id: worker}


@app.post("/register_worker")
async def register_worker(worker: Worker):
    ts = int(time.time())
    worker.register_ts = ts
    worker.last_available_ts = ts
    WORKERS[worker.worker_id] = worker
    db.update_worker(worker)
    return {"message": f"Worker {worker.worker_id} registered successfully"}


@app.post("/submit_task")
async def submit_task(task: Task):
    """
    Submit a new task to the task queue (in this example, ZMQ).
    """
    await mq.send_task_to_queue(task)
    db.update_task(task)
    return {
        "status": "success",
        "message": "Task submitted successfully",
        "task_id": task.task_id,
    }


@app.post("/worker_update")
async def worker_update(worker: Worker):
    """
    Update worker status.
    Worker will send this request to master in every 10 seconds automatically.
    """
    ts = int(time.time())
    worker.last_available_ts = ts
    WORKERS[worker.worker_id] = worker
    db.update_worker(worker)
    logger.info(f"Update worker {worker.worker_id} status: {worker.status}")
    return {"message": "Worker status updated successfully"}


@app.post("/task_update")
async def task_update(task_status: List[ContainerStatus]):
    """
    Update task status.
    Worker will send this request to master in every 10 seconds automatically.
    """
    db.update_task_status(task_status)
    return {"message": "Tasks status updated successfully"}


@app.get("/worker_panel")
async def worker_panel():
    """
    Get worker panel.
    Show all workers' status and quota.
    """
    return {"workers": WORKERS}


def health_check():
    """
    loop check the worker status
    if worker last_available_ts is 60 seconds ago, then set worker status to unavailable
    if worker last_available_ts is 3600 seconds ago, then remove worker from WORKERS
    """
    while True:
        for worker_id, worker in list(WORKERS.items()):
            ts = int(time.time())
            if ts - worker.last_available_ts > 60:
                worker.status = "Unavailable"
                logger.info(f"Worker {worker_id} status: {worker.status}")
                WORKERS[worker.worker_id] = worker
            if ts - worker.last_available_ts > 3600:
                WORKERS.pop(worker_id)
                logger.info(f"Worker {worker_id} is removed")

        time.sleep(10)


threading.Thread(target=health_check, daemon=True).start()
