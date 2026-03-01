from __future__ import annotations

import base64
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Header, HTTPException, Query, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


class PatientLite(BaseModel):
    firstName: str
    lastName: str
    dob: Optional[str] = None  # "YYYY-MM-DD" preferred


class AppointmentLite(BaseModel):
    type: Optional[str] = None
    date: str
    time: Optional[str] = None
    durationMinutes: Optional[int] = None
    provider: Optional[str] = None
    location: Optional[str] = None


class CurrentAppointmentLite(BaseModel):
    date: str
    time: str


class Job(BaseModel):
    id: str
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    status: str = "QUEUED"  # QUEUED | CLAIMED | DONE | FAILED
    claimedAt: Optional[float] = None
    completedAt: Optional[float] = None


class JobEnqueueRequest(BaseModel):
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class ScheduleStartRequest(BaseModel):
    patient: PatientLite
    appointment: AppointmentLite
    idempotencyKey: Optional[str] = None
    waitSeconds: Optional[int] = None  # 0 => return immediately without waiting


class RescheduleStartRequest(BaseModel):
    patient: PatientLite
    currentAppointment: CurrentAppointmentLite
    appointment: AppointmentLite
    idempotencyKey: Optional[str] = None
    waitSeconds: Optional[int] = None


class CancelStartRequest(BaseModel):
    patient: PatientLite
    currentAppointment: CurrentAppointmentLite
    idempotencyKey: Optional[str] = None
    waitSeconds: Optional[int] = None


class GetPatientAppointmentsStartRequest(BaseModel):
    patient: PatientLite
    idempotencyKey: Optional[str] = None
    waitSeconds: Optional[int] = None


class JobResult(BaseModel):
    ok: bool
    action: str
    jobId: str
    details: Dict[str, Any] = Field(default_factory=dict)


JOBS: List[Job] = []
RESULTS: Dict[str, JobResult] = {}
IDEMPOTENCY_TO_JOB_ID: Dict[str, str] = {}

TERMINAL_STATUSES = {"DONE", "FAILED"}
DEFAULT_START_WAIT_SECONDS = 120
WAIT_POLL_SECONDS = 0.25
API_PREFIX = "/api/v1"

app = FastAPI(title="Athena Extension Test Backend", version="0.8")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _find_job(job_id: str) -> Optional[Job]:
    for j in JOBS:
        if j.id == job_id:
            return j
    return None


def _enqueue_or_get_idempotent_job(
    idem: str, job_type: str, payload: Dict[str, Any]
) -> Tuple[Job, bool]:
    existing_id = IDEMPOTENCY_TO_JOB_ID.get(idem)
    if existing_id:
        existing_job = _find_job(existing_id)
        if existing_job:
            return existing_job, True

    job = Job(id=str(uuid.uuid4()), type=job_type, payload=payload)
    JOBS.append(job)
    IDEMPOTENCY_TO_JOB_ID[idem] = job.id
    return job, False


def _wait_for_terminal_result(job_id: str, timeout_seconds: int) -> Optional[JobResult]:
    deadline = time.time() + max(0, timeout_seconds)
    while time.time() < deadline:
        job = _find_job(job_id)
        if job and job.status in TERMINAL_STATUSES:
            return RESULTS.get(job_id)
        time.sleep(WAIT_POLL_SECONDS)
    return None


def _start_response(job: Job, idempotent: bool, wait_seconds: Optional[int]):
    timeout = DEFAULT_START_WAIT_SECONDS if wait_seconds is None else max(0, wait_seconds)

    # Backward-compatible mode: no wait
    if timeout == 0:
        return {
            "ok": True,
            "job": job,
            "idempotent": idempotent,
            "completed": job.status in TERMINAL_STATUSES,
            "status": job.status,
            "result": RESULTS.get(job.id),
        }

    result = _wait_for_terminal_result(job.id, timeout)
    if result is None:
        # Timed out waiting; job still in progress
        current = _find_job(job.id) or job
        return {
            "ok": True,
            "job": current,
            "idempotent": idempotent,
            "completed": False,
            "timedOut": True,
            "status": current.status,
            "result": RESULTS.get(current.id),
        }

    current = _find_job(job.id) or job
    return {
        "ok": result.ok,
        "job": current,
        "idempotent": idempotent,
        "completed": True,
        "status": current.status,
        "result": result,
    }


@app.get(f"{API_PREFIX}/health")
def health():
    return {
        "ok": True,
        "queued": sum(1 for j in JOBS if j.status == "QUEUED"),
        "claimed": sum(1 for j in JOBS if j.status == "CLAIMED"),
        "done": sum(1 for j in JOBS if j.status in TERMINAL_STATUSES),
        "total": len(JOBS),
    }


@app.post(f"{API_PREFIX}/jobs/enqueue")
def enqueue_job(req: JobEnqueueRequest, authorization: Optional[str] = Header(default=None)):
    job = Job(id=str(uuid.uuid4()), type=req.type, payload=req.payload)
    JOBS.append(job)
    return {"ok": True, "job": job}


@app.post(f"{API_PREFIX}/schedule/start")
def schedule_start(req: ScheduleStartRequest):
    idem = req.idempotencyKey or str(uuid.uuid4())
    payload = {
        "patient": req.patient.model_dump(),
        "appointment": req.appointment.model_dump(),
        "idempotencyKey": idem,
        "waitSeconds": req.waitSeconds,
    }
    job, idempotent = _enqueue_or_get_idempotent_job(idem, "SCHEDULE_APPOINTMENT", payload)
    return _start_response(job, idempotent=idempotent, wait_seconds=req.waitSeconds)


@app.post(f"{API_PREFIX}/reschedule/start")
def reschedule_start(req: RescheduleStartRequest):
    idem = req.idempotencyKey or str(uuid.uuid4())
    payload = {
        "patient": req.patient.model_dump(),
        "currentAppointment": req.currentAppointment.model_dump(),
        "appointment": req.appointment.model_dump(),
        "idempotencyKey": idem,
        "waitSeconds": req.waitSeconds,
    }
    job, idempotent = _enqueue_or_get_idempotent_job(idem, "RESCHEDULE_APPOINTMENT", payload)
    return _start_response(job, idempotent=idempotent, wait_seconds=req.waitSeconds)


@app.post(f"{API_PREFIX}/cancel/start")
def cancel_start(req: CancelStartRequest):
    idem = req.idempotencyKey or str(uuid.uuid4())
    payload = {
        "patient": req.patient.model_dump(),
        "currentAppointment": req.currentAppointment.model_dump(),
        "idempotencyKey": idem,
        "waitSeconds": req.waitSeconds,
    }
    job, idempotent = _enqueue_or_get_idempotent_job(idem, "CANCEL_APPOINTMENT", payload)
    return _start_response(job, idempotent=idempotent, wait_seconds=req.waitSeconds)


@app.post(f"{API_PREFIX}/appointments/start")
@app.post(f"{API_PREFIX}/get-patient-appointments/start")
def get_patient_appointments_start(req: GetPatientAppointmentsStartRequest):
    idem = req.idempotencyKey or str(uuid.uuid4())
    payload = {
        "patient": req.patient.model_dump(),
        "idempotencyKey": idem,
        "waitSeconds": req.waitSeconds,
    }
    job, idempotent = _enqueue_or_get_idempotent_job(idem, "GET_PATIENT_APPOINTMENTS", payload)
    return _start_response(job, idempotent=idempotent, wait_seconds=req.waitSeconds)


@app.post(f"{API_PREFIX}/upload-document/start")
async def upload_document_start(
    firstName: str = Form(...),
    lastName: str = Form(...),
    dob: Optional[str] = Form(None),
    documentClass: str = Form("ADMIN"),
    idempotencyKey: Optional[str] = Form(None),
    waitSeconds: Optional[int] = Form(None),
    file: UploadFile = File(...),
):
    idem = idempotencyKey or str(uuid.uuid4())

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    file_b64 = base64.b64encode(content).decode("utf-8")
    mime_type = file.content_type or "application/octet-stream"
    file_name = file.filename or "upload"

    payload = {
        "patient": {
            "firstName": firstName,
            "lastName": lastName,
            "dob": dob,
        },
        "document": {
            "base64": file_b64,
            "fileName": file_name,
            "mimeType": mime_type,
            "documentClass": documentClass,
        },
        "idempotencyKey": idem,
        "waitSeconds": waitSeconds,
    }

    job, idempotent = _enqueue_or_get_idempotent_job(idem, "UPLOAD_PATIENT_DOCUMENT", payload)
    return _start_response(job, idempotent=idempotent, wait_seconds=waitSeconds)


@app.get(f"{API_PREFIX}/jobs/next")
def claim_next_job(
    authorization: Optional[str] = Header(default=None),
    reclaim_after_seconds: int = Query(default=120, ge=1),
):
    now = time.time()

    for job in JOBS:
        if job.status == "CLAIMED" and job.claimedAt and (now - job.claimedAt) > reclaim_after_seconds:
            job.claimedAt = now
            return {"ok": True, "job": job, "reclaimed": True}

    for job in JOBS:
        if job.status == "QUEUED":
            job.status = "CLAIMED"
            job.claimedAt = now
            return {"ok": True, "job": job, "reclaimed": False}

    return {"ok": True, "job": None}


@app.post(f"{API_PREFIX}/jobs/{{job_id}}/complete")
def complete_job(job_id: str, result: JobResult, authorization: Optional[str] = Header(default=None)):
    RESULTS[job_id] = result
    job = _find_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job.completedAt = time.time()
    job.status = "DONE" if result.ok else "FAILED"
    return {"ok": True}


@app.get(f"{API_PREFIX}/jobs/{{job_id}}")
def get_job(job_id: str):
    job = _find_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": job, "result": RESULTS.get(job_id)}


@app.get(f"{API_PREFIX}/jobs")
def list_jobs():
    return {"ok": True, "jobs": JOBS}


@app.get(f"{API_PREFIX}/results")
def list_results():
    return {"ok": True, "results": RESULTS}


@app.post(f"{API_PREFIX}/reset")
def reset_state():
    JOBS.clear()
    RESULTS.clear()
    IDEMPOTENCY_TO_JOB_ID.clear()
    return {"ok": True}
