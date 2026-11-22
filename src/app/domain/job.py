from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import override
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


class StaleAttempt(InvalidTransition):
    """Итог пришёл от попытки, которая уже не текущая: например, от Tool до retry."""

    def __init__(
        self,
        job_id: UUID,
        status: JobStatus,
        action: JobAction,
        attempt: int,
        current_attempt: int,
    ) -> None:
        super().__init__(job_id, status, action)
        self.attempt: int = attempt
        self.current_attempt: int = current_attempt

    @property
    @override
    def details(self) -> dict[str, str]:
        return {
            **super().details,
            "attempt": str(self.attempt),
            "current_attempt": str(self.current_attempt),
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
    # Сквозной номер текущей попытки воркера; при retry не обнуляется. 0 — не запускалось.
    attempt: int = 0
    # Последний номер попытки до текущего запуска: после retry очередь снова считает
    # свои попытки с 1, а сквозной номер продолжается с attempt_base + 1.
    attempt_base: int = 0
    # До какого момента текущая попытка может быть жива; позже Dispatcher уже прервал Tool.
    attempt_deadline: datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def can(self, action: JobAction) -> bool:
        allowed_from, _ = TRANSITIONS[action]
        return self.status in allowed_from

    def run(self, at: datetime | None = None) -> None:
        """created → running: воркер забрал задание (первая попытка)."""
        self._transition(JobAction.RUN)
        self.attempt = self.attempt_base + 1
        self.attempt_deadline = None
        self.started_at = at or _now()

    def start_attempt(
        self, job_try: int, lease: timedelta, at: datetime | None = None
    ) -> int:
        """Воркер начинает попытку job_try (номер очереди, с 1 в каждом запуске) на срок lease.

        Из created — переход run. В running — только следующая попытка (повтор ARQ после
        сбоя или первый запуск после retry) и только когда предыдущая точно не работает:
        её срок истёк. Иначе в одном Job работали бы два Tool — отклоняется (З1).
        Возвращает сквозной номер попытки: с ним воркер сообщает итог в finish или fail.
        """
        now = at or _now()
        attempt = self.attempt_base + job_try
        if not self._can_start_next_attempt(attempt, now):
            self._transition(
                JobAction.RUN
            )  # created → running, иначе InvalidTransition
        # Попытка внутри running — не переход (job-lifecycle.md), статус не меняется.
        self.attempt = attempt
        self.attempt_deadline = now + lease
        self.started_at = now
        return attempt

    def _can_start_next_attempt(self, attempt: int, now: datetime) -> bool:
        if self.status is not JobStatus.RUNNING or attempt <= self.attempt:
            return False
        return self.attempt_deadline is None or now >= self.attempt_deadline

    def finish(
        self, result: object, *, attempt: int, at: datetime | None = None
    ) -> None:
        """running → finished: Tool текущей попытки attempt вернул результат."""
        self._check_attempt(JobAction.FINISH, attempt)
        self._transition(JobAction.FINISH)
        self.attempt_deadline = None
        self.result = result
        self.finished_at = at or _now()

    def fail(
        self,
        error: Mapping[str, object],
        *,
        attempt: int | None,
        at: datetime | None = None,
    ) -> None:
        """running → failed: ошибка Tool попытки attempt или исчерпаны попытки.

        attempt=None — итог текущей попытки, которая сама не ответила (воркер упал).
        """
        self._check_attempt(JobAction.FAIL, attempt)
        self._transition(JobAction.FAIL)
        self.attempt_deadline = None
        self.error = dict(error)
        self.finished_at = at or _now()

    def cancel(self, at: datetime | None = None) -> None:
        """created | running → cancelled: отмена пользователем."""
        self._transition(JobAction.CANCEL)
        self.attempt_deadline = None
        self.finished_at = at or _now()

    def retry(self) -> None:
        """failed → running: прежняя ошибка очищается, задание снова ждёт воркера."""
        self._transition(JobAction.RETRY)
        self.error = None
        self.finished_at = None
        # Задание снова ждёт воркера. Попытки до retry больше не текущие: их поздний итог
        # отклоняется, а новые продолжат сквозную нумерацию.
        self.attempt_base = self.attempt
        self.attempt_deadline = None
        self.started_at = None

    def delete(self, at: datetime | None = None) -> None:
        """Мягкое удаление — не переход: статус не меняется (job-lifecycle.md)."""
        if self.status not in DELETABLE_STATUSES:
            raise JobActive(self.id, self.status)
        self.deleted_at = at or _now()

    def _check_attempt(self, action: JobAction, attempt: int | None) -> None:
        # Поздний итог старой попытки (например, Tool, запущенного до retry) не закрывает
        # задание: итог принимается только от текущей попытки текущего запуска.
        if self.status is not JobStatus.RUNNING:
            return  # недопустимый статус отклонит _transition
        has_current = self.attempt > self.attempt_base
        if not has_current or (attempt is not None and attempt != self.attempt):
            raise StaleAttempt(self.id, self.status, action, attempt or 0, self.attempt)

    def _transition(self, action: JobAction) -> None:
        allowed_from, target = TRANSITIONS[action]
        if self.status not in allowed_from:
            raise InvalidTransition(self.id, self.status, action)
        self.status = target
