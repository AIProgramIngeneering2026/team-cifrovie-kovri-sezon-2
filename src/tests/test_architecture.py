import ast
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[1] / "app"

# Ядро: Job, Registry, Dispatcher, контракт Tool и сами Tool.
CORE = [
    *sorted((APP / "domain").rglob("*.py")),
    APP / "registry.py",
    APP / "dispatcher.py",
    *sorted((APP / "tools").rglob("*.py")),
]
FORBIDDEN = {
    "fastapi", "starlette", "uvicorn", "sqlalchemy", "alembic", "asyncpg", "psycopg",
    "psycopg2", "sqlite3", "redis", "arq", "httpx", "requests", "aiohttp",
}  # fmt: skip


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)
    return {name.split(".")[0] for name in names}


@pytest.mark.parametrize("path", CORE, ids=lambda p: str(p.relative_to(APP)))
def test_core_does_not_import_framework_or_database(path: Path) -> None:
    assert path.exists()
    assert imported_modules(path) & FORBIDDEN == set()
