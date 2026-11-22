import asyncio
from typing import override

from app.domain.job import JobStatus
from app.domain.tool import ToolContext, ToolError
from app.tools.asleep import AsleepTool
from tests.fakes import (
    BoomTool,
    EchoParams,
    EchoResult,
    EchoTool,
    InMemoryTasks,
    make_dispatcher,
    make_job,
)

S = JobStatus


def test_s1_2_job_goes_from_created_through_running_to_finished() -> None:
    async def scenario() -> None:
        job = make_job("asleep", {"seconds": 1})
        tasks = InMemoryTasks(job)
        assert job.status == S.CREATED
        await make_dispatcher(tasks, AsleepTool()).dispatch(str(job.id), 1)
        assert job.status == S.FINISHED
        assert job.result == {"slept_seconds": 1}
        assert job.error is None
        assert job.started_at is not None and job.finished_at is not None
        assert job.started_at <= job.finished_at

    asyncio.run(scenario())


def test_success_saves_tool_result_and_keeps_params() -> None:
    async def scenario() -> None:
        job = make_job(params={"text": "hi"})
        tool = EchoTool()
        await make_dispatcher(InMemoryTasks(job), tool).dispatch(str(job.id), 1)
        assert (job.status, job.result, tool.calls) == (S.FINISHED, {"text": "hi"}, 1)
        assert job.params == {"text": "hi"}  # параметры задания не изменились

    asyncio.run(scenario())


def test_e2_invalid_params_fail_job_with_field_name_and_tool_not_called() -> None:
    async def scenario() -> None:
        job = make_job(params={"text": 5, "extra": True})
        tool = EchoTool()
        await make_dispatcher(InMemoryTasks(job), tool).dispatch(str(job.id), 1)
        assert (job.status, tool.calls) == (S.FAILED, 0)
        assert job.error is not None
        assert job.error["code"] == "invalid_params"
        assert job.error["details"] == {"fields": ["text", "extra"]}

    asyncio.run(scenario())


def test_unknown_operation_fails_job_with_unknown_type() -> None:
    async def scenario() -> None:
        job = make_job("resize")
        await make_dispatcher(InMemoryTasks(job), EchoTool()).dispatch(str(job.id), 1)
        assert job.status == S.FAILED
        assert job.error is not None
        assert job.error["code"] == "unknown_type"
        assert job.error["details"] == {"type": "resize"}

    asyncio.run(scenario())


def test_tool_crash_fails_job_without_leaking_details() -> None:
    async def scenario() -> None:
        job = make_job("boom")
        await make_dispatcher(InMemoryTasks(job), BoomTool()).dispatch(str(job.id), 1)
        assert job.status == S.FAILED
        assert job.error is not None
        assert job.error["code"] == "internal_error"
        assert "ошибка в коде Tool" not in str(job.error)

    asyncio.run(scenario())


def test_tool_refusal_saves_tool_error() -> None:
    class RefusingTool(EchoTool):
        @override
        async def run(self, params: EchoParams, ctx: ToolContext) -> EchoResult:
            raise ToolError("too_large", "Файл больше лимита", {"max_bytes": 10})

    async def scenario() -> None:
        job = make_job()
        await make_dispatcher(InMemoryTasks(job), RefusingTool()).dispatch(
            str(job.id), 1
        )
        assert job.status == S.FAILED
        assert job.error == {
            "code": "too_large",
            "message": "Файл больше лимита",
            "details": {"max_bytes": 10},
        }

    asyncio.run(scenario())
