import pytest

from app.registry import DuplicateToolError, Registry, UnknownToolError
from app.tools.asleep import AsleepTool
from app.tools.fetch import FetchTool


def test_get_registered_tool_by_type() -> None:
    fetch = FetchTool()
    registry = Registry([AsleepTool(), fetch])
    assert registry.get("fetch") is fetch
    assert registry.types() == ["asleep", "fetch"]


def test_get_unknown_type_is_explicit_error() -> None:
    registry = Registry([AsleepTool()])
    with pytest.raises(UnknownToolError) as exc_info:
        registry.get("resize")
    assert exc_info.value.code == "unknown_type"
    assert exc_info.value.type_ == "resize"
    assert isinstance(exc_info.value, KeyError)  # контракт: неизвестный тип — KeyError


def test_register_duplicate_type_fails() -> None:
    registry = Registry([AsleepTool()])
    with pytest.raises(DuplicateToolError):
        registry.register(AsleepTool())
    assert registry.types() == ["asleep"]
