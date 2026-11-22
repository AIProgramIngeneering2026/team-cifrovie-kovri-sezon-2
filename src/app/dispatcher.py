import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from pydantic import BaseModel, ValidationError

from app.domain.tool import HttpAdapter, ToolError
from app.registry import Registry, UnknownToolError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobToRun:
    """Что Dispatcher знает о задании: тип, сохранённые параметры и номер попытки."""

    id: str
    type: str
    params: Mapping[str, object]
    # Сквозной номер попытки от Job (Job.start_attempt): с ним сообщается итог.
    attempt: int


class TaskService(Protocol):
    """Смена статуса задания — только через TaskService → JobLifecycle (architecture §2).

    Недопустимый переход TaskService отклоняет сам, без исключения: например, finish после
    отмены оставляет задание в cancelled, а результат отбрасывается (job-lifecycle.md).
    """

    async def start(
        self, job_id: str, attempt: int, lease: timedelta
    ) -> JobToRun | None:
        """Начать попытку attempt на срок lease (Job.start_attempt).

        None — запускать нельзя: задание отменено, завершено, удалено или предыдущая
        попытка ещё может работать.
        """
        ...

    async def finish(self, job_id: str, result: object, attempt: int) -> None:
        """Итог попытки attempt; итог устаревшей попытки Job отклоняет (StaleAttempt)."""
        ...

    async def fail(
        self, job_id: str, error: Mapping[str, object], attempt: int | None
    ) -> None:
        """attempt=None — итог текущей попытки, которая сама не ответила (worker_lost)."""
        ...


class RetryAttempt(Exception):
    """Попытка не удалась, но попытки ещё есть: очередь повторит задание внутри running."""


@dataclass(frozen=True)
class _RunContext:
    job_id: str
    attempt: int
    http: HttpAdapter


class Dispatcher:
    """Выполняет задание из очереди: статус → Tool из Registry → исход (architecture §3)."""

    def __init__(
        self,
        tasks: TaskService,
        registry: Registry,
        http: HttpAdapter,
        max_attempts: int,
    ) -> None:
        self._tasks: TaskService = tasks
        self._registry: Registry = registry
        self._http: HttpAdapter = http
        self._max_attempts: int = max_attempts

    async def dispatch(self, job_id: str, attempt: int) -> None:
        """Обработать сообщение очереди; attempt — номер попытки ARQ, начиная с 1.

        asyncio.CancelledError (отмена задания) не перехватывается: статус cancelled
        уже записан до прерывания (ADR-0002).
        """
        if attempt > self._max_attempts:
            # Предыдущая попытка не вернулась: воркер упал во время выполнения.
            await self._fail(
                job_id,
                None,
                "worker_lost",
                "Воркер аварийно завершился, попытки исчерпаны",
            )
            return

        job = await self._tasks.start(job_id, attempt, self._attempt_lease())
        if job is None:
            logger.info(
                "job %s: не запускается — статус не допускает выполнения", job_id
            )
            return

        try:
            tool = self._registry.get(job.type)
        except UnknownToolError as exc:
            await self._fail(
                job_id,
                job.attempt,
                exc.code,
                "Тип операции не поддерживается",
                {"type": job.type},
            )
            return

        try:
            params = tool.params_model.model_validate(job.params)
        except ValidationError as exc:
            fields = [".".join(str(p) for p in e["loc"]) for e in exc.errors()]
            await self._fail(
                job_id,
                job.attempt,
                "invalid_params",
                "Сохранённые параметры не подходят под схему",
                {"fields": fields},
            )
            return

        ctx = _RunContext(job_id=job_id, attempt=attempt, http=self._http)
        try:
            async with asyncio.timeout(tool.timeout_for(params)):
                result: BaseModel = await tool.run(params, ctx)
        except TimeoutError:
            if attempt < self._max_attempts:
                logger.warning("job %s: таймаут попытки %s, повтор", job_id, attempt)
                raise RetryAttempt from None
            await self._fail(
                job_id,
                job.attempt,
                "timeout",
                "Превышено время выполнения, попытки исчерпаны",
            )
            return
        except ToolError as exc:
            await self._tasks.fail(job_id, exc.to_dict(), job.attempt)
            return
        except Exception:
            logger.exception("job %s: непредвиденная ошибка Tool %s", job_id, job.type)
            await self._fail(
                job_id, job.attempt, "internal_error", "Внутренняя ошибка исполнителя"
            )
            return

        await self._tasks.finish(job_id, result.model_dump(mode="json"), job.attempt)

    def _attempt_lease(self) -> timedelta:
        """Дольше самого большого timeout_seconds попытка не живёт: run прерывается по timeout_for."""
        timeouts = [
            self._registry.get(t).timeout_seconds for t in self._registry.types()
        ]
        return timedelta(seconds=max(timeouts, default=0))

    async def _fail(
        self,
        job_id: str,
        attempt: int | None,
        code: str,
        message: str,
        details: Mapping[str, object] | None = None,
    ) -> None:
        error = ToolError(code, message, dict(details or {})).to_dict()
        await self._tasks.fail(job_id, error, attempt)
