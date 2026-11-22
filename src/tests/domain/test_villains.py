import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from app.dispatcher import RetryAttempt
from app.domain.job import InvalidTransition, JobStatus
from tests.fakes import EchoTool, InMemoryTasks, SlowTool, make_dispatcher, make_job

S = JobStatus
LEASE = timedelta(seconds=5)
# Попытка, начатая давно: её срок истёк, воркер с ней точно не работает.
LONG_AGO = datetime(2026, 1, 1, tzinfo=UTC)


def test_z1_second_tool_in_same_job_is_rejected_by_job() -> None:
    job = make_job()
    job.start_attempt(1, LEASE)
    with pytest.raises(InvalidTransition):
        job.start_attempt(1, LEASE)  # тот же номер попытки — второй Tool параллельно
    assert (job.status, job.attempt) == (S.RUNNING, 1)


def test_z1_next_attempt_is_rejected_while_previous_may_still_run() -> None:
    job = make_job()
    job.start_attempt(1, LEASE)
    with pytest.raises(InvalidTransition):
        job.start_attempt(2, LEASE)  # срок первой попытки не истёк — она может работать
    assert (job.status, job.attempt) == (S.RUNNING, 1)


def test_z1_next_attempt_while_first_tool_runs_does_not_start_second_tool() -> None:
    async def scenario() -> None:
        job = make_job()
        tasks = InMemoryTasks(job)
        tool = EchoTool(gate=asyncio.Event())
        dispatcher = make_dispatcher(tasks, tool)

        first = asyncio.create_task(dispatcher.dispatch(str(job.id), 1))
        await tool.started.wait()
        await dispatcher.dispatch(str(job.id), 2)  # повтор ARQ, пока первый Tool жив
        assert tool.gate is not None
        tool.gate.set()
        await first

        assert (tool.calls, tool.max_running) == (1, 1)
        assert (job.status, job.attempt) == (S.FINISHED, 1)
        assert tasks.rejected == ["run from running"]

    asyncio.run(scenario())


def test_z1_duplicate_delivery_while_running_does_not_start_second_tool() -> None:
    async def scenario() -> None:
        job = make_job()
        tasks = InMemoryTasks(job)
        tool = EchoTool(gate=asyncio.Event())
        dispatcher = make_dispatcher(tasks, tool)

        first = asyncio.create_task(dispatcher.dispatch(str(job.id), 1))
        await tool.started.wait()
        await dispatcher.dispatch(
            str(job.id), 1
        )  # повторная доставка того же сообщения
        assert tool.gate is not None
        tool.gate.set()
        await first

        assert (tool.calls, tool.max_running) == (1, 1)
        assert job.status == S.FINISHED
        assert tasks.rejected == ["run from running"]

    asyncio.run(scenario())


def test_z2_redelivery_after_worker_crash_finishes_job() -> None:
    async def scenario() -> None:
        job = make_job()
        job.start_attempt(1, LEASE, at=LONG_AGO)  # воркер взял задание и упал
        tasks = InMemoryTasks(job)
        tool = EchoTool()
        await make_dispatcher(tasks, tool).dispatch(str(job.id), 2)
        assert (job.status, job.attempt, tool.calls) == (S.FINISHED, 2, 1)

    asyncio.run(scenario())


def test_z2_attempts_exhausted_after_crash_fails_job() -> None:
    async def scenario() -> None:
        job = make_job()
        job.start_attempt(3, LEASE, at=LONG_AGO)  # последняя попытка не вернулась
        tasks = InMemoryTasks(job)
        tool = EchoTool()
        await make_dispatcher(tasks, tool, max_attempts=3).dispatch(str(job.id), 4)
        assert job.status == S.FAILED
        assert job.error is not None and job.error["code"] == "worker_lost"
        assert tool.calls == 0

    asyncio.run(scenario())


def test_z2_timeout_on_last_attempt_fails_job() -> None:
    async def scenario() -> None:
        job = make_job("slow")
        tasks = InMemoryTasks(job)
        dispatcher = make_dispatcher(tasks, SlowTool(), max_attempts=2)
        with pytest.raises(RetryAttempt):
            await dispatcher.dispatch(str(job.id), 1)
        assert job.status == S.RUNNING  # попытки остались — повтор внутри running
        await dispatcher.dispatch(str(job.id), 2)
        assert job.status == S.FAILED
        assert job.error is not None and job.error["code"] == "timeout"

    asyncio.run(scenario())


def test_z3_cancelled_job_cannot_become_finished() -> None:
    job = make_job()
    job.run()
    job.cancel()
    with pytest.raises(InvalidTransition):
        job.finish({"text": "hi"}, attempt=job.attempt)
    assert (job.status, job.result) == (S.CANCELLED, None)


def test_z3_late_tool_result_after_cancel_is_dropped() -> None:
    async def scenario() -> None:
        job = make_job()
        tasks = InMemoryTasks(job)
        tool = EchoTool(gate=asyncio.Event())
        running = asyncio.create_task(
            make_dispatcher(tasks, tool).dispatch(str(job.id), 1)
        )
        await tool.started.wait()
        job.cancel()  # пользователь отменил, пока Tool работал
        assert tool.gate is not None
        tool.gate.set()
        await running
        assert (job.status, job.result) == (S.CANCELLED, None)
        assert tasks.rejected == ["finish from cancelled"]

    asyncio.run(scenario())


@pytest.mark.parametrize("final", [S.FINISHED, S.FAILED, S.CANCELLED])
def test_z3_deleted_job_cannot_become_finished(final: JobStatus) -> None:
    async def scenario() -> None:
        job = make_job()
        job.run()
        match final:
            case S.FINISHED:
                job.finish({"text": "hi"}, attempt=job.attempt)
            case S.FAILED:
                job.fail(
                    {"code": "internal_error", "message": "…", "details": {}},
                    attempt=job.attempt,
                )
            case _:
                job.cancel()
        job.delete()
        tasks = InMemoryTasks(job)
        tool = EchoTool()
        await make_dispatcher(tasks, tool).dispatch(str(job.id), 2)
        with pytest.raises(InvalidTransition):
            job.finish({"text": "late"}, attempt=job.attempt)
        assert (job.status, tool.calls) == (final, 0)

    asyncio.run(scenario())


def test_z4_failed_job_cannot_become_finished() -> None:
    job = make_job()
    job.run()
    job.fail(
        {"code": "network_error", "message": "…", "details": {}}, attempt=job.attempt
    )
    with pytest.raises(InvalidTransition):
        job.finish({"text": "hi"}, attempt=job.attempt)
    assert job.status == S.FAILED


def test_z4_tool_result_from_before_retry_does_not_close_job() -> None:
    async def scenario() -> None:
        job = make_job()
        tasks = InMemoryTasks(job)
        old_tool = EchoTool(gate=asyncio.Event())
        old = asyncio.create_task(
            make_dispatcher(tasks, old_tool).dispatch(str(job.id), 1)
        )
        await old_tool.started.wait()
        # Попытка 1 зависла: задание записали в failed, пользователь сделал retry.
        job.fail({"code": "timeout", "message": "…", "details": {}}, attempt=None)
        job.retry()
        # Очередь снова начинает с попытки 1 — новый Tool уже работает.
        new_tool = EchoTool(gate=asyncio.Event())
        new = asyncio.create_task(
            make_dispatcher(tasks, new_tool).dispatch(str(job.id), 1)
        )
        await new_tool.started.wait()

        assert old_tool.gate is not None and new_tool.gate is not None
        old_tool.gate.set()
        await old  # старый Tool вернул результат позже, чем стартовал новый
        assert (job.status, job.result) == (S.RUNNING, None)
        assert tasks.rejected == ["finish from running"]

        new_tool.gate.set()
        await new
        assert (job.status, job.attempt) == (S.FINISHED, 2)

    asyncio.run(scenario())


def test_z4_redelivery_after_fail_does_not_run_tool() -> None:
    async def scenario() -> None:
        job = make_job()
        job.run()
        job.fail(
            {"code": "network_error", "message": "…", "details": {}},
            attempt=job.attempt,
        )
        tasks = InMemoryTasks(job)
        tool = EchoTool()
        await make_dispatcher(tasks, tool).dispatch(str(job.id), 2)
        assert (job.status, tool.calls) == (S.FAILED, 0)
        assert tasks.rejected == ["run from failed"]

    asyncio.run(scenario())


def test_z5_job_cannot_finish_without_running() -> None:
    job = make_job()
    with pytest.raises(InvalidTransition):
        job.finish({"text": "hi"}, attempt=job.attempt)
    assert (job.status, job.result) == (S.CREATED, None)


def test_z5_finish_request_for_created_job_is_rejected() -> None:
    async def scenario() -> None:
        job = make_job()
        tasks = InMemoryTasks(job)
        await tasks.finish(str(job.id), {"text": "hi"}, 1)
        assert job.status == S.CREATED
        assert tasks.rejected == ["finish from created"]

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["run", "start_attempt", "retry"])
def test_z6_finished_job_cannot_run_again(action: str) -> None:
    job = make_job()
    job.run()
    job.finish({"text": "hi"}, attempt=job.attempt)
    with pytest.raises(InvalidTransition):
        match action:
            case "run":
                job.run()
            case "start_attempt":
                job.start_attempt(job.attempt + 1, LEASE)
            case _:
                job.retry()
    assert job.status == S.FINISHED


def test_z6_redelivery_of_finished_job_does_not_run_tool_again() -> None:
    async def scenario() -> None:
        job = make_job()
        tasks = InMemoryTasks(job)
        tool = EchoTool()
        dispatcher = make_dispatcher(tasks, tool)
        await dispatcher.dispatch(str(job.id), 1)
        await dispatcher.dispatch(str(job.id), 2)  # сообщение доставлено ещё раз
        assert (job.status, tool.calls) == (S.FINISHED, 1)

    asyncio.run(scenario())
