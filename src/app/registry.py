from collections.abc import Iterable
from typing import Any

from app.domain.tool import Tool

type AnyTool = Tool[Any, Any]  # pyright: ignore[reportExplicitAny]


class UnknownToolError(KeyError):
    """Нет Tool для типа операции: при создании задания — 422."""

    code: str = "unknown_type"

    def __init__(self, type_: str) -> None:
        super().__init__(type_)
        self.type_: str = type_


class DuplicateToolError(ValueError):
    """Тип операции уже зарегистрирован — ошибка конфигурации при старте."""

    def __init__(self, type_: str) -> None:
        super().__init__(f"Tool для типа {type_!r} уже зарегистрирован")
        self.type_: str = type_


class Registry:
    """Находит Tool по типу операции; новый тип добавляется регистрацией."""

    def __init__(self, tools: Iterable[AnyTool] = ()) -> None:
        self._tools: dict[str, AnyTool] = {}
        for tool in tools:
            self.register(tool)

    def register(self, tool: AnyTool) -> None:
        if tool.type in self._tools:
            raise DuplicateToolError(tool.type)
        self._tools[tool.type] = tool

    def get(self, type_: str) -> AnyTool:
        try:
            return self._tools[type_]
        except KeyError:
            raise UnknownToolError(type_) from None

    def types(self) -> list[str]:
        return sorted(self._tools)
