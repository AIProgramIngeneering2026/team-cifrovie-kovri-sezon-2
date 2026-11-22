from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.domain.job import (
    TRANSITIONS,
    InvalidTransition,
    Job,
    JobAction,
    JobActive,
    JobStatus,
)

S = JobStatus
A = JobAction
T0 = datetime(2026, 11, 22, 12, 0, tzinfo=UTC)

# Ожидаемая таблица из docs/job-lifecycle.md, «Разрешённые переходы».
ALLOWED = {
    (S.CREATED, A.RUN): S.RUNNING,
    (S.RUNNING, A.FINISH): S.FINISHED,
    (S.RUNNING, A.FAIL): S.FAILED,
    (S.CREATED, A.CANCEL): S.CANCELLED,
    (S.RUNNING, A.CANCEL): S.CANCELLED,
    (S.FAILED, A.RETRY): S.RUNNING,
}
ALL_PAIRS = [(s, a) for s in S for a in A]


def make_job(status: JobStatus = S.CREATED) -> Job:
    return Job(
        user_id=uuid4(),
        name="pause",
        type="asleep",
        params={"seconds": 10},
        idempotency_key=str(uuid4()),
        status=status,
    )


def apply(job: Job, action: JobAction) -> None:
    match action:
        case A.RUN:
            job.run(T0)
        case A.FINISH:
            job.finish({"slept_seconds": 10}, T0)
        case A.FAIL:
            job.fail({"code": "timeout", "message": "…", "details": {}}, T0)
        case A.CANCEL:
            job.cancel(T0)
        case A.RETRY:
            job.retry(T0)


def test_c1_transition_table_matches_lifecycle_doc() -> None:
    table = {
        (src, action): target
        for action, (sources, target) in TRANSITIONS.items()
        for src in sources
    }
    assert table == ALLOWED


@pytest.mark.parametrize(("status", "action"), [p for p in ALL_PAIRS if p in ALLOWED])
def test_c1_allowed_transition_changes_status(
    status: JobStatus, action: JobAction
) -> None:
    job = make_job(status)
    apply(job, action)
    assert job.status == ALLOWED[(status, action)]


@pytest.mark.parametrize(
    ("status", "action"), [p for p in ALL_PAIRS if p not in ALLOWED]
)
def test_c1_forbidden_transition_raises_and_keeps_status(
    status: JobStatus, action: JobAction
) -> None:
    job = make_job(status)
    with pytest.raises(InvalidTransition) as exc_info:
        apply(job, action)
    err = exc_info.value
    assert job.status == status
    assert err.code == "invalid_transition"
    assert err.details == {"task_id": str(job.id), "status": status, "action": action}


def test_c1_cannot_return_to_created() -> None:
    assert all(target != S.CREATED for _, target in TRANSITIONS.values())


def test_c1_happy_path_fills_timestamps_and_result() -> None:
    job = make_job()
    job.run(T0)
    job.finish({"slept_seconds": 10}, T0)
    assert (job.status, job.started_at, job.finished_at) == (S.FINISHED, T0, T0)
    assert job.result == {"slept_seconds": 10}
    assert job.is_terminal


def test_c1_retry_clears_previous_error() -> None:
    job = make_job()
    job.run(T0)
    job.fail({"code": "network_error", "message": "…", "details": {}}, T0)
    assert job.error is not None and not job.is_terminal
    job.retry(T0)
    assert (job.status, job.error, job.finished_at) == (S.RUNNING, None, None)


def test_c1_late_result_after_cancel_is_rejected() -> None:
    # Пользователь отменил задание, пока Tool завершался: результат отбрасывается.
    job = make_job()
    job.run(T0)
    job.cancel(T0)
    with pytest.raises(InvalidTransition):
        job.finish({"slept_seconds": 10})
    assert (job.status, job.result) == (S.CANCELLED, None)


@pytest.mark.parametrize("status", [S.FINISHED, S.FAILED, S.CANCELLED])
def test_delete_inactive_job_keeps_status(status: JobStatus) -> None:
    job = make_job(status)
    job.delete(T0)
    assert (job.status, job.deleted_at) == (status, T0)


@pytest.mark.parametrize("status", [S.CREATED, S.RUNNING])
def test_delete_active_job_is_rejected(status: JobStatus) -> None:
    job = make_job(status)
    with pytest.raises(JobActive) as exc_info:
        job.delete(T0)
    assert exc_info.value.code == "task_active"
    assert (job.status, job.deleted_at) == (status, None)
