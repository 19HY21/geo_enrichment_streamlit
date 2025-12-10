# -*- coding: utf-8 -*-
"""
簡易ジョブキュー（シングルプロセス・スレッドワーカー）

- submit_job でジョブを投入し、IDを返す
- get_job で状態/進捗/出力パスを参照
- ワーカーはバックグラウンドスレッドで実行
"""
from __future__ import annotations

import os
import threading
import queue
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .tasks import run_geocode_job, BATCH_SIZE_DEFAULT


@dataclass
class Job:
    id: str
    status: str = "queued"  # queued | running | done | error
    progress: float = 0.0
    message: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    output_path: Optional[str] = None
    cache_path: Optional[str] = None
    output_name: Optional[str] = None
    error: Optional[str] = None


class JobQueue:
    def __init__(self):
        self.jobs: Dict[str, Job] = {}
        self.q: "queue.Queue[Job]" = queue.Queue()
        self.lock = threading.Lock()
        self.worker = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker.start()

    def submit_job(
        self,
        *,
        input_path: str,
        zip_cols: list[str],
        addr_cols: list[str],
        batch_size: int = BATCH_SIZE_DEFAULT,
        uploaded_cache: Optional[dict] = None,
        sheet_name: Optional[str] = None,
    ) -> str:
        job_id = str(uuid.uuid4())
        job = Job(
            id=job_id,
            params={
                "input_path": input_path,
                "zip_cols": zip_cols,
                "addr_cols": addr_cols,
                "batch_size": batch_size,
                "uploaded_cache": uploaded_cache,
                "sheet_name": sheet_name,
            },
            status="queued",
            message="queued",
        )
        with self.lock:
            self.jobs[job_id] = job
        self.q.put(job)
        return job_id

    def get_job(self, job_id: str) -> Optional[Job]:
        with self.lock:
            return self.jobs.get(job_id)

    # 内部利用: 進捗更新
    def _update_progress(self, job: Job, done: int, total: int, phase: str, message: str):
        pct = 0.0
        if total:
            pct = min(max(done / total, 0.0), 1.0)
        job.progress = pct
        job.message = f"{phase}: {message}"

    def _worker_loop(self):
        while True:
            job = self.q.get()
            with self.lock:
                job.status = "running"
                job.message = "started"

            def progress_cb(done, total, phase, message):
                with self.lock:
                    self._update_progress(job, done, total, phase, message)

            try:
                result = run_geocode_job(
                    job.params["input_path"],
                    job.params["zip_cols"],
                    job.params["addr_cols"],
                    batch_size=job.params["batch_size"],
                    uploaded_cache=job.params["uploaded_cache"],
                    sheet_name=job.params["sheet_name"],
                    progress_cb=progress_cb,
                    job_id=job.id,
                )
                with self.lock:
                    job.status = "done"
                    job.output_path = result["output_path"]
                    job.cache_path = result["cache_path"]
                    job.output_name = result["output_name"]
                    job.progress = 1.0
                    job.message = "done"
            except Exception as e:
                with self.lock:
                    job.status = "error"
                    job.error = repr(e)
                    job.message = "error"
                    job.progress = 0.0
            finally:
                self.q.task_done()


# シングルトンとして利用
queue_instance = JobQueue()
