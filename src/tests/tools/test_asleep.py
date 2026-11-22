import asyncio

import pytest
from pydantic import ValidationError

from app.tools.asleep import AsleepParams, AsleepTool


def error_fields(exc: ValidationError) -> list[str]:
    return [".".join(str(p) for p in e["loc"]) for e in exc.errors()]


@pytest.mark.parametrize("seconds", [1, 3600])
def test_asleep_accepts_seconds_on_boundaries(seconds: int) -> None:
    assert AsleepParams.model_validate({"seconds": seconds}).seconds == seconds


@pytest.mark.parametrize("seconds", [0, 3601, True, "5", 1.5, None])
def test_e2_asleep_rejects_invalid_seconds_with_field_name(seconds: object) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _ = AsleepParams.model_validate({"seconds": seconds})
    assert error_fields(exc_info.value) == ["seconds"]


@pytest.mark.parametrize(
    ("params", "field"), [({}, "seconds"), ({"seconds": 1, "minutes": 1}, "minutes")]
)
def test_e2_asleep_rejects_missing_and_unknown_params(
    params: dict[str, object], field: str
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _ = AsleepParams.model_validate(params)
    assert error_fields(exc_info.value) == [field]


@pytest.mark.parametrize(("seconds", "timeout"), [(1, 31), (10, 40), (3600, 3630)])
def test_asleep_timeout_is_seconds_plus_30(seconds: int, timeout: int) -> None:
    tool = AsleepTool()
    assert tool.timeout_for(AsleepParams(seconds=seconds)) == timeout
    assert tool.timeout_for(AsleepParams(seconds=seconds)) <= tool.timeout_seconds


def test_asleep_returns_slept_seconds() -> None:
    result = asyncio.run(AsleepTool().run(AsleepParams(seconds=1), ctx=None))  # pyright: ignore[reportArgumentType]
    assert result.slept_seconds == 1
