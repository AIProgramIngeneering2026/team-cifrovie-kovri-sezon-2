import asyncio
import json
from typing import ClassVar, Literal, override

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    JsonValue,
    RootModel,
    field_validator,
)

from app.domain.tool import HttpAdapter, HttpAdapterError, Tool, ToolContext

MAX_RESPONSE_BYTES = 1024 * 1024
MAX_TEXT_BYTES = 64 * 1024

type FetchErrorCode = Literal["http_status", "network_error", "timeout", "too_large"]


class FetchParams(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    urls: list[HttpUrl] = Field(min_length=1, max_length=20)

    @field_validator("urls")
    @classmethod
    def _unique(cls, urls: list[HttpUrl]) -> list[HttpUrl]:
        if len({str(u) for u in urls}) != len(urls):
            raise ValueError("адреса не должны повторяться")
        return urls


class FetchError(BaseModel):
    code: FetchErrorCode
    message: str


class FetchItem(BaseModel):
    url: str
    ok: bool
    status_code: int | None
    content_type: str | None
    body: JsonValue
    error: FetchError | None


class FetchResult(RootModel[list[FetchItem]]):
    """Ответы в порядке params.urls; недоступный URL — элемент с ok=false."""


class FetchTool(Tool[FetchParams, FetchResult]):
    """Параллельный GET; ошибки по URL — часть результата (tool-contract §7)."""

    type: ClassVar[str] = "fetch"
    params_model: ClassVar[type[BaseModel]] = FetchParams
    result_model: ClassVar[type[BaseModel]] = FetchResult
    timeout_seconds: ClassVar[int] = 120

    @override
    async def run(self, params: FetchParams, ctx: ToolContext) -> FetchResult:
        # TaskGroup отменяет остальные запросы при отмене задания или ошибке в коде.
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(_fetch_one(ctx.http, str(url))) for url in params.urls
            ]
        return FetchResult([t.result() for t in tasks])


async def _fetch_one(http: HttpAdapter, url: str) -> FetchItem:
    try:
        response = await http.get(url, max_bytes=MAX_RESPONSE_BYTES)
    except HttpAdapterError as exc:
        code: FetchErrorCode = (
            "too_large"
            if exc.code == "too_large"
            else ("timeout" if exc.code == "timeout" else "network_error")
        )
        return _failed(url, None, None, code, exc.message)

    if not 200 <= response.status_code < 300:
        return _failed(
            url,
            response.status_code,
            response.content_type,
            "http_status",
            f"Сервер ответил {response.status_code}",
        )
    return FetchItem(
        url=url,
        ok=True,
        status_code=response.status_code,
        content_type=response.content_type,
        body=_parse_body(response.content, response.content_type),
        error=None,
    )


def _failed(
    url: str,
    status_code: int | None,
    content_type: str | None,
    code: FetchErrorCode,
    message: str,
) -> FetchItem:
    return FetchItem(
        url=url,
        ok=False,
        status_code=status_code,
        content_type=content_type,
        body=None,
        error=FetchError(code=code, message=message),
    )


def _parse_body(content: bytes, content_type: str | None) -> JsonValue:
    if content_type and "json" in content_type.lower():
        try:
            parsed: JsonValue = json.loads(content)  # pyright: ignore[reportAny]
        except ValueError:
            pass
        else:
            return parsed
    return content[:MAX_TEXT_BYTES].decode("utf-8", errors="replace")
