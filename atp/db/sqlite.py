import sqlite3
from typing import List
from abc import ABC, abstractmethod

from commons.commons import Task, Worker, TaskStatus


class DBUtil(ABC):
    @abstractmethod
    def create_tables(self):
        pass

    @abstractmethod
    def update_worker(self):
        pass

    @abstractmethod
    def update_task(self):
        pass

    @abstractmethod
    def update_task_status(self):
        pass


class SQLiteUtil(DBUtil):
    def __init__(self, db_path=":memory:"):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def create_tables(self):
        # Create Worker table
        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS workers (
            worker_id TEXT PRIMARY KEY,
            status TEXT,
            cpu REAL,
            cpu_usage REAL,
            mem REAL,
            mem_usage REAL,
            disk REAL,
            disk_usage REAL,
            gpu TEXT,
            register_ts INTEGER,
            last_available_ts INTEGER
        )
        """
        )

        # Create Task table
        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            image_name TEXT,
            command TEXT,
            ports TEXT,
            cpu_request REAL,
            mem_request REAL,
            disk_request REAL,
            gpu_request INTEGER,
            gpu_mem_request REAL,
            status TEXT,
            worker_id TEXT,
            FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
        )
        """
        )

        self.conn.commit()

    def update_worker(self, worker: Worker):
        self.cursor.execute(
            """
        INSERT OR REPLACE INTO workers (worker_id, status, cpu, cpu_usage, mem, mem_usage, disk, disk_usage, gpu, register_ts, last_available_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                worker.worker_id,
                worker.status,
                worker.workload.cpu,
                worker.workload.cpu_usage,
                worker.workload.mem,
                worker.workload.mem_usage,
                worker.workload.disk,
                worker.workload.disk_usage,
                str(worker.workload.gpu),
                worker.register_ts,
                worker.last_available_ts,
            ),
        )
        self.conn.commit()

    def update_task(self, task: Task):
        self.cursor.execute(
            """
        INSERT INTO tasks (task_id, image_name, command, ports, cpu_request, mem_request, disk_request, gpu_request, gpu_mem_request, status, worker_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                task.task_id,
                task.image_name,
                task.command,
                ",".join(map(str, task.ports or [])),
                task.cpu_request,
                task.mem_request,
                task.disk_request,
                task.gpu_request,
                task.gpu_mem_request,
                None,
                None,
            ),
        )
        self.conn.commit()

    def update_task_status(self, task_status: List[TaskStatus]):
        for st in task_status:
            self.cursor.execute(
                """
            UPDATE tasks SET status=?, worker_id=? WHERE task_id=?
            """,
                (st.status, st.worker_id, st.container_name),
            )
        self.conn.commit()
