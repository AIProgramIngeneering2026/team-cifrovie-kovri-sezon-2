import asyncio

import pytest
from pydantic import ValidationError

from app.domain.tool import HttpAdapterError
from app.tools.asleep import AsleepParams
from app.tools.fetch import FetchParams, FetchTool
from tests.fakes import FakeHttp, FakeResponse, InMemoryTasks, make_dispatcher, make_job


def error_fields(exc: ValidationError) -> list[str]:
    return [".".join(str(p) for p in e["loc"]) for e in exc.errors()]


@pytest.mark.parametrize("seconds", [1, 2, 3599, 3600])
def test_asleep_accepts_seconds_in_range(seconds: int) -> None:
    assert AsleepParams.model_validate({"seconds": seconds}).seconds == seconds


@pytest.mark.parametrize("seconds", [0, -1, 3601, True, "5", 1.5, None])
def test_e2_asleep_rejects_invalid_seconds_with_field_name(seconds: object) -> None:
    with pytest.raises(ValidationError) as exc_info:
        AsleepParams.model_validate({"seconds": seconds})
    assert error_fields(exc_info.value) == ["seconds"]


@pytest.mark.parametrize(
    ("params", "field"),
    [({}, "seconds"), ({"seconds": 1, "minutes": 1}, "minutes")],
)
def test_e2_asleep_rejects_missing_and_unknown_params(
    params: dict[str, object], field: str
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        AsleepParams.model_validate(params)
    assert error_fields(exc_info.value) == [field]


@pytest.mark.parametrize("count", [1, 20])
def test_fetch_accepts_url_count_on_boundaries(count: int) -> None:
    urls = [f"https://example.com/{i}" for i in range(count)]
    assert len(FetchParams.model_validate({"urls": urls}).urls) == count


@pytest.mark.parametrize(
    ("params", "field"),
    [
        ({"urls": []}, "urls"),
        ({"urls": [f"https://example.com/{i}" for i in range(21)]}, "urls"),
        ({"urls": ["https://example.com/", "https://example.com/"]}, "urls"),
        ({"urls": ["https://example.com/", "ftp://example.com/file"]}, "urls.1"),
        ({"urls": ["https://example.com/"], "timeout": 5}, "timeout"),
        ({}, "urls"),
    ],
)
def test_e2_fetch_rejects_invalid_params_with_field_name(
    params: dict[str, object], field: str
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        FetchParams.model_validate(params)
    assert error_fields(exc_info.value) == [field]


def test_s5_1_unavailable_url_is_part_of_result_and_job_finishes() -> None:
    async def scenario() -> None:
        ok, down, missing = (
            "https://api.example.com/a",
            "https://down.example.com/",
            "https://api.example.com/404",
        )
        http = FakeHttp(
            {
                ok: FakeResponse(ok, 200, "application/json", b'{"value": 1}'),
                down: HttpAdapterError("network_error", "Connection refused"),
                missing: FakeResponse(missing, 404, "text/html", b"not found"),
            }
        )
        params = {"urls": [ok, down, missing]}
        job = make_job("fetch", params)
        await make_dispatcher(InMemoryTasks(job), FetchTool(), http=http).dispatch(
            str(job.id), 1
        )
        assert job.status == "finished"
        assert job.result == [
            {
                "url": ok,
                "ok": True,
                "status_code": 200,
                "content_type": "application/json",
                "body": {"value": 1},
                "error": None,
            },
            {
                "url": down,
                "ok": False,
                "status_code": None,
                "content_type": None,
                "body": None,
                "error": {"code": "network_error", "message": "Connection refused"},
            },
            {
                "url": missing,
                "ok": False,
                "status_code": 404,
                "content_type": "text/html",
                "body": None,
                "error": {"code": "http_status", "message": "Сервер ответил 404"},
            },
        ]
        assert job.params == params  # входные параметры не изменились

    asyncio.run(scenario())


def test_s5_2_all_urls_failed_job_still_finishes() -> None:
    async def scenario() -> None:
        job = make_job("fetch", {"urls": ["https://a.example/", "https://b.example/"]})
        await make_dispatcher(InMemoryTasks(job), FetchTool()).dispatch(str(job.id), 1)
        assert job.status == "finished"
        assert isinstance(job.result, list)
        assert [item["ok"] for item in job.result] == [False, False]  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]

    asyncio.run(scenario())
