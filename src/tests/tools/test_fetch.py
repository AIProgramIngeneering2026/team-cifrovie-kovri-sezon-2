import asyncio
from collections.abc import Mapping
from dataclasses import dataclass

import pytest
from pydantic import ValidationError

from app.dispatcher import Dispatcher, JobToRun
from app.domain.tool import HttpAdapterError, HttpResponse, HttpStream
from app.registry import Registry
from app.tools.fetch import FetchParams, FetchTool


@dataclass
class Response:
    url: str
    status_code: int
    content_type: str | None
    content: bytes


class FakeHttp:
    def __init__(self, routes: Mapping[str, Response | HttpAdapterError]) -> None:
        self.routes: dict[str, Response | HttpAdapterError] = dict(routes)

    async def get(self, url: str, *, max_bytes: int) -> HttpResponse:
        route = self.routes.get(url, HttpAdapterError("network_error", "нет маршрута"))
        if isinstance(route, HttpAdapterError):
            raise route
        return route

    def stream(self, url: str, *, max_bytes: int) -> HttpStream:
        raise NotImplementedError


class Tasks:
    """Минимальный TaskService: запоминает, чем закончилось задание."""

    def __init__(self, params: Mapping[str, object]) -> None:
        self.params: dict[str, object] = dict(params)
        self.outcome: tuple[str, object] | None = None

    async def start(self, job_id: str) -> JobToRun | None:
        return JobToRun(id=job_id, type="fetch", params=self.params)

    async def finish(self, job_id: str, result: object) -> None:
        self.outcome = ("finished", result)

    async def fail(self, job_id: str, error: Mapping[str, object]) -> None:
        self.outcome = ("failed", dict(error))


def run_fetch(urls: list[str], http: FakeHttp) -> tuple[str, object] | None:
    tasks = Tasks({"urls": urls})
    dispatcher = Dispatcher(tasks, Registry([FetchTool()]), http, max_attempts=3)
    asyncio.run(dispatcher.dispatch("job-1", 1))
    return tasks.outcome


def test_s5_1_one_unavailable_url_is_part_of_result_and_job_finishes() -> None:
    ok, down = "https://api.example.com/a", "https://down.example.com/"
    http = FakeHttp(
        {
            ok: Response(ok, 200, "application/json", b'{"value": 1}'),
            down: HttpAdapterError("network_error", "Connection refused"),
        }
    )
    assert run_fetch([ok, down], http) == (
        "finished",
        [
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
        ],
    )


def test_s5_2_all_urls_failed_job_still_finishes() -> None:
    outcome = run_fetch(["https://a.example/", "https://b.example/"], FakeHttp({}))
    assert outcome is not None
    status, result = outcome
    assert status == "finished"
    assert isinstance(result, list)
    assert [item["ok"] for item in result] == [False, False]  # pyright: ignore[reportUnknownVariableType]


def test_fetch_http_error_status_and_too_large_are_item_errors() -> None:
    missing, big = "https://api.example.com/404", "https://big.example.com/"
    http = FakeHttp(
        {
            missing: Response(missing, 404, "text/html", b"not found"),
            big: HttpAdapterError("too_large", "больше 1 МБ"),
        }
    )
    outcome = run_fetch([missing, big], http)
    assert outcome is not None and outcome[0] == "finished"
    result = outcome[1]
    assert isinstance(result, list)
    assert [item["error"]["code"] for item in result] == ["http_status", "too_large"]  # pyright: ignore[reportUnknownVariableType]


@pytest.mark.parametrize(
    ("params", "field"),
    [
        ({"urls": []}, "urls"),
        ({"urls": [f"https://example.com/{i}" for i in range(21)]}, "urls"),
        ({"urls": ["https://example.com/", "https://example.com/"]}, "urls"),
        ({"urls": ["ftp://example.com/file"]}, "urls.0"),
        ({"urls": ["https://example.com/"], "timeout": 5}, "timeout"),
    ],
)
def test_e2_fetch_rejects_invalid_params_with_field_name(
    params: dict[str, object], field: str
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _ = FetchParams.model_validate(params)
    assert [".".join(str(p) for p in e["loc"]) for e in exc_info.value.errors()] == [
        field
    ]
