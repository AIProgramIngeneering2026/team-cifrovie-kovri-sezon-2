from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import ClassVar, Protocol

from pydantic import BaseModel


class HttpResponse(Protocol):
    """Ответ, полностью прочитанный адаптером (не больше max_bytes)."""

    url: str  # итоговый адрес после редиректов
    status_code: int
    content_type: str | None
    content: bytes


class HttpStream(AbstractAsyncContextManager["HttpStream"], Protocol):
    """Потоковый ответ: заголовки доступны сразу, тело читается частями.

    Закрытие контекста (в том числе при отмене) закрывает соединение.
    """

    url: str
    status_code: int
    content_type: str | None
    content_length: int | None  # из заголовка Content-Length, если он есть

    def iter_bytes(self) -> AsyncIterator[bytes]:
        """Части тела; при превышении max_bytes — HttpAdapterError("too_large")."""
        ...


class HttpAdapterError(Exception):
    """Сбой внешнего запроса после всех повторов адаптера.

    Tool превращает его в ToolError или в элемент результата (tool-contract §4).
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code: str = code  # network_error | too_large
        self.message: str = message


class HttpAdapter(Protocol):
    """Единственный путь Tool во внешнюю сеть: таймаут, ограниченный retry, лимит размера."""

    async def get(self, url: str, *, max_bytes: int) -> HttpResponse: ...

    def stream(self, url: str, *, max_bytes: int) -> HttpStream: ...


class ToolContext(Protocol):
    """То, что Dispatcher передаёт Tool помимо параметров."""

    @property
    def job_id(self) -> str:  # для логов и correlation ID
        ...

    @property
    def attempt(self) -> int:  # номер попытки ARQ, начиная с 1
        ...

    @property
    def http(self) -> HttpAdapter:  # внешние запросы — только через него
        ...


class ToolError(Exception):
    """Отказ исполнителя: задание переходит в failed, ошибка сохраняется в Job.error."""

    def __init__(
        self, code: str, message: str, details: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.code: str = code
        self.message: str = message
        self.details: dict[str, object] = details or {}

    def to_dict(self) -> dict[str, object]:
        """Формат Job.error: {code, message, details}."""
        return {"code": self.code, "message": self.message, "details": self.details}


class Tool[P: BaseModel, R: BaseModel](ABC):
    """Исполнитель одного типа операции.

    Наследник объявляет type, params_model, result_model, timeout_seconds и реализует run.
    """

    # Тип операции, ключ в Registry: "asleep", "fetch", ...
    type: ClassVar[str]
    # Схема параметров — по ней проверяет TaskService.
    params_model: ClassVar[type[BaseModel]]
    # Схема результата — сохраняется в Job.result.
    result_model: ClassVar[type[BaseModel]]
    # Верхний предел одной попытки при любых параметрах.
    timeout_seconds: ClassVar[int]

    def timeout_for(self, params: P) -> int:
        """Предел попытки для этих параметров; не больше timeout_seconds."""
        _ = params
        return self.timeout_seconds

    @abstractmethod
    async def run(self, params: P, ctx: ToolContext) -> R:
        """Выполнить операцию.

        Получает уже проверенные параметры. Возвращает результат или
        выбрасывает ToolError. asyncio.CancelledError не перехватывает.
        """
