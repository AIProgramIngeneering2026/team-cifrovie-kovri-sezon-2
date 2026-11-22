from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4


class JobStatus(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobAction(StrEnum):
    RUN = "run"
    FINISH = "finish"
    FAIL = "fail"
    CANCEL = "cancel"
    RETRY = "retry"


# Единственная таблица допустимых переходов (docs/job-lifecycle.md): действие → (из, в).
TRANSITIONS: Mapping[JobAction, tuple[frozenset[JobStatus], JobStatus]] = {
    JobAction.RUN: (frozenset({JobStatus.CREATED}), JobStatus.RUNNING),
    JobAction.FINISH: (frozenset({JobStatus.RUNNING}), JobStatus.FINISHED),
    JobAction.FAIL: (frozenset({JobStatus.RUNNING}), JobStatus.FAILED),
    JobAction.CANCEL: (
        frozenset({JobStatus.CREATED, JobStatus.RUNNING}),
        JobStatus.CANCELLED,
    ),
    JobAction.RETRY: (frozenset({JobStatus.FAILED}), JobStatus.RUNNING),
}

TERMINAL_STATUSES = frozenset({JobStatus.FINISHED, JobStatus.CANCELLED})
# Удалить можно только задание, которое не выполняется и не ждёт воркера (ADR-0003).
DELETABLE_STATUSES = frozenset(
    {JobStatus.FINISHED, JobStatus.FAILED, JobStatus.CANCELLED}
)


class InvalidTransition(Exception):
    """Переход не разрешён из текущего статуса; в API — 409 invalid_transition."""

    code: str = "invalid_transition"

    def __init__(self, job_id: UUID, status: JobStatus, action: JobAction) -> None:
        super().__init__(f"Нельзя выполнить {action} для задания в статусе {status}")
        self.job_id: UUID = job_id
        self.status: JobStatus = status
        self.action: JobAction = action

    @property
    def details(self) -> dict[str, str]:
        return {
            "task_id": str(self.job_id),
            "status": self.status,
            "action": self.action,
        }


class JobActive(Exception):
    """Удаление задания в created или running; в API — 409 task_active."""

    code: str = "task_active"

    def __init__(self, job_id: UUID, status: JobStatus) -> None:
        super().__init__(
            f"Нельзя удалить задание в статусе {status}: сначала отмените его"
        )
        self.job_id: UUID = job_id
        self.status: JobStatus = status


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass
class Job:
    """Задание и его статус. Статус меняется только методами переходов."""

    user_id: UUID
    name: str
    type: str
    params: dict[str, object]
    idempotency_key: str
    schedule_id: UUID | None = None
    id: UUID = field(default_factory=uuid4)
    status: JobStatus = JobStatus.CREATED
    result: object | None = None
    error: dict[str, object] | None = None
    created_at: datetime = field(default_factory=_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    deleted_at: datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def can(self, action: JobAction) -> bool:
        allowed_from, _ = TRANSITIONS[action]
        return self.status in allowed_from

    def run(self, at: datetime | None = None) -> None:
        """created → running: воркер забрал задание."""
        self._transition(JobAction.RUN)
        self.started_at = at or _now()

    def finish(self, result: object, at: datetime | None = None) -> None:
        """running → finished: Tool вернул результат."""
        self._transition(JobAction.FINISH)
        self.result = result
        self.finished_at = at or _now()

    def fail(self, error: Mapping[str, object], at: datetime | None = None) -> None:
        """running → failed: ошибка Tool или исчерпаны попытки."""
        self._transition(JobAction.FAIL)
        self.error = dict(error)
        self.finished_at = at or _now()

    def cancel(self, at: datetime | None = None) -> None:
        """created | running → cancelled: отмена пользователем."""
        self._transition(JobAction.CANCEL)
        self.finished_at = at or _now()

    def retry(self, at: datetime | None = None) -> None:
        """failed → running: прежняя ошибка очищается, задание снова ждёт воркера."""
        self._transition(JobAction.RETRY)
        self.error = None
        self.finished_at = None
        self.started_at = at or _now()

    def delete(self, at: datetime | None = None) -> None:
        """Мягкое удаление — не переход: статус не меняется (job-lifecycle.md)."""
        if self.status not in DELETABLE_STATUSES:
            raise JobActive(self.id, self.status)
        self.deleted_at = at or _now()

    def _transition(self, action: JobAction) -> None:
        allowed_from, target = TRANSITIONS[action]
        if self.status not in allowed_from:
            raise InvalidTransition(self.id, self.status, action)
        self.status = target
