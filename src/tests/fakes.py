import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar, override
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from app.dispatcher import Dispatcher, JobToRun
from app.domain.job import InvalidTransition, Job
from app.domain.tool import (
    HttpAdapterError,
    HttpResponse,
    HttpStream,
    Tool,
    ToolContext,
)
from app.registry import AnyTool, Registry


def make_job(type_: str = "echo", params: Mapping[str, object] | None = None) -> Job:
    return Job(
        user_id=uuid4(),
        name="test",
        type=type_,
        params=dict(params if params is not None else {"text": "hi"}),
        idempotency_key=str(uuid4()),
    )


class InMemoryTasks:
    """TaskService для тестов: статус меняют только методы Job.

    Отклонённый переход, как у воркера в job-lifecycle.md, не бросает исключение,
    а записывается в rejected.
    """

    def __init__(self, *jobs: Job) -> None:
        self.jobs: dict[str, Job] = {str(j.id): j for j in jobs}
        self.rejected: list[str] = []

    async def start(self, job_id: str, attempt: int) -> JobToRun | None:
        job = self.jobs.get(job_id)
        if job is None or job.deleted_at is not None:
            return None
        try:
            job.start_attempt(attempt)
        except InvalidTransition as exc:
            self.rejected.append(f"{exc.action} from {exc.status}")
            return None
        return JobToRun(id=job_id, type=job.type, params=job.params)

    async def finish(self, job_id: str, result: object) -> None:
        try:
            self.jobs[job_id].finish(result)
        except InvalidTransition as exc:
            self.rejected.append(f"{exc.action} from {exc.status}")

    async def fail(self, job_id: str, error: Mapping[str, object]) -> None:
        try:
            self.jobs[job_id].fail(error)
        except InvalidTransition as exc:
            self.rejected.append(f"{exc.action} from {exc.status}")


class EchoParams(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    text: str


class EchoResult(BaseModel):
    text: str


class EchoTool(Tool[EchoParams, EchoResult]):
    """Фейковый Tool: считает запуски и может ждать сигнала, чтобы «работать долго»."""

    type: ClassVar[str] = "echo"
    params_model: ClassVar[type[BaseModel]] = EchoParams
    result_model: ClassVar[type[BaseModel]] = EchoResult
    timeout_seconds: ClassVar[int] = 5

    def __init__(self, gate: asyncio.Event | None = None) -> None:
        self.gate: asyncio.Event | None = gate
        self.started: asyncio.Event = asyncio.Event()
        self.calls: int = 0
        self.running: int = 0
        self.max_running: int = 0

    @override
    async def run(self, params: EchoParams, ctx: ToolContext) -> EchoResult:
        self.calls += 1
        self.running += 1
        self.max_running = max(self.max_running, self.running)
        self.started.set()
        try:
            if self.gate is not None:
                await self.gate.wait()
            return EchoResult(text=params.text)
        finally:
            self.running -= 1


class BoomTool(EchoTool):
    type: ClassVar[str] = "boom"

    @override
    async def run(self, params: EchoParams, ctx: ToolContext) -> EchoResult:
        self.calls += 1
        raise RuntimeError("ошибка в коде Tool")


class SlowTool(EchoTool):
    """Не укладывается в свой timeout_seconds."""

    type: ClassVar[str] = "slow"
    timeout_seconds: ClassVar[int] = 0

    @override
    async def run(self, params: EchoParams, ctx: ToolContext) -> EchoResult:
        self.calls += 1
        await asyncio.sleep(1)
        return EchoResult(text=params.text)


@dataclass
class FakeResponse:
    url: str
    status_code: int
    content_type: str | None
    content: bytes


class FakeHttp:
    """HttpAdapter по таблице url → ответ или ошибка адаптера."""

    def __init__(self, routes: Mapping[str, FakeResponse | HttpAdapterError]) -> None:
        self.routes: dict[str, FakeResponse | HttpAdapterError] = dict(routes)
        self.requested: list[str] = []

    async def get(self, url: str, *, max_bytes: int) -> HttpResponse:
        self.requested.append(url)
        route = self.routes.get(url, HttpAdapterError("network_error", "нет маршрута"))
        if isinstance(route, HttpAdapterError):
            raise route
        return route

    def stream(self, url: str, *, max_bytes: int) -> HttpStream:
        raise NotImplementedError


def make_dispatcher(
    tasks: InMemoryTasks,
    *tools: AnyTool,
    http: FakeHttp | None = None,
    max_attempts: int = 3,
) -> Dispatcher:
    return Dispatcher(tasks, Registry(tools), http or FakeHttp({}), max_attempts)
