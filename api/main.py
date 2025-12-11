# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import shutil
import tempfile
from typing import List, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from geo_logic.job_queue import queue_instance
from geo_logic.tasks import BATCH_SIZE_DEFAULT

app = FastAPI(title="Geo Enrichment Job API")


def _parse_cols(val: Optional[str]) -> List[str]:
    if not val:
        return []
    return [v.strip() for v in val.split(",") if v.strip()]


@app.post("/jobs")
async def create_job(
    file: UploadFile = File(...),
    zip_cols: str = Form(""),
    addr_cols: str = Form(""),
    batch_size: int = Form(BATCH_SIZE_DEFAULT),
    cache_file: Optional[UploadFile] = File(None),
    sheet_name: Optional[str] = Form(None),
):
    tmpdir = tempfile.mkdtemp(prefix="geo_job_")
    input_path = os.path.join(tmpdir, file.filename)
    with open(input_path, "wb") as f:
        f.write(await file.read())

    uploaded_cache = None
    if cache_file is not None:
        try:
            raw = json.loads((await cache_file.read()).decode("utf-8"))
            uploaded_cache = {k: tuple(v) if isinstance(v, list) else tuple(v) for k, v in raw.items()}
        except Exception:
            pass

    job_id = queue_instance.submit_job(
        input_path=input_path,
        zip_cols=_parse_cols(zip_cols),
        addr_cols=_parse_cols(addr_cols),
        batch_size=batch_size or BATCH_SIZE_DEFAULT,
        uploaded_cache=uploaded_cache,
        sheet_name=sheet_name,
    )
    return {"job_id": job_id}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = queue_instance.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {
        "id": job.id,
        "status": job.status,
        "progress": job.progress,
        "message": job.message,
        "output_name": job.output_name,
    }


@app.get("/jobs/{job_id}/result")
def download_result(job_id: str):
    job = queue_instance.get_job(job_id)
    if job is None or job.output_path is None:
        raise HTTPException(status_code=404, detail="result not ready")
    if not os.path.exists(job.output_path):
        raise HTTPException(status_code=404, detail="result file missing")
    return FileResponse(job.output_path, filename=job.output_name or os.path.basename(job.output_path))


@app.get("/jobs/{job_id}/cache")
def download_cache(job_id: str):
    job = queue_instance.get_job(job_id)
    if job is None or job.cache_path is None:
        raise HTTPException(status_code=404, detail="cache not ready")
    if not os.path.exists(job.cache_path):
        raise HTTPException(status_code=404, detail="cache file missing")
    return FileResponse(job.cache_path, filename=os.path.basename(job.cache_path))


@app.delete("/jobs/{job_id}")
def cleanup_job(job_id: str):
    job = queue_instance.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    # best-effort cleanup of temp dir if possible
    if job.output_path:
        try:
            tmpdir = os.path.dirname(job.output_path)
            if os.path.isdir(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass
    return JSONResponse({"ok": True})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=False)