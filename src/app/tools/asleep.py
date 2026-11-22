import asyncio
from typing import ClassVar, override

from pydantic import BaseModel, ConfigDict, Field

from app.domain.tool import Tool, ToolContext

MAX_SECONDS = 3600


class AsleepParams(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    seconds: int = Field(ge=1, le=MAX_SECONDS, description="Сколько секунд ждать.")


class AsleepResult(BaseModel):
    slept_seconds: int


class AsleepTool(Tool[AsleepParams, AsleepResult]):
    """Ожидание; отмена прерывает его сразу (tool-contract §7)."""

    type: ClassVar[str] = "asleep"
    params_model: ClassVar[type[BaseModel]] = AsleepParams
    result_model: ClassVar[type[BaseModel]] = AsleepResult
    # Контракт задаёт seconds + 30, но timeout_seconds — атрибут класса,
    # поэтому берём верхнюю границу для максимального seconds.
    timeout_seconds: ClassVar[int] = MAX_SECONDS + 30

    @override
    async def run(self, params: AsleepParams, ctx: ToolContext) -> AsleepResult:
        await asyncio.sleep(params.seconds)
        return AsleepResult(slept_seconds=params.seconds)
