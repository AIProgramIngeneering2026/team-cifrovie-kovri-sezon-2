# Контракт Tool

Tool — исполнитель одного типа операции. Задание (Job) хранит тип, параметры, статус и результат;
Tool только выполняет операцию: получает параметры, возвращает результат или отказывает с ошибкой.
`Registry` находит Tool по типу задания, `Dispatcher` запускает его (см. [architecture.md](architecture.md)).

Контракт одинаков для всех Tool, поэтому `Dispatcher` запускает любой Tool одним и тем же кодом, а новый тип
операции добавляется регистрацией нового Tool без правки диспетчера.

## 1. Интерфейс

```python
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Protocol

from pydantic import BaseModel


class HttpAdapter(Protocol):
    """Единственный путь Tool во внешнюю сеть: таймаут, ограниченный retry, лимит размера."""

    async def get(self, url: str, *, max_bytes: int) -> "HttpResponse": ...
    def stream(self, url: str, *, max_bytes: int) -> "HttpStream": ...


class ToolContext(Protocol):
    """То, что Dispatcher передаёт Tool помимо параметров."""

    job_id: str            # для логов и correlation ID
    attempt: int           # номер попытки ARQ, начиная с 1
    http: HttpAdapter      # внешние запросы — только через него


class Tool[P: BaseModel, R: BaseModel](ABC):
    type: ClassVar[str]                 # тип операции, ключ в Registry: "asleep", "fetch", ...
    params_model: ClassVar[type[BaseModel]]  # схема параметров — по ней проверяет TaskService
    result_model: ClassVar[type[BaseModel]]  # схема результата — сохраняется в Job.result
    timeout_seconds: ClassVar[int]      # верхний предел одной попытки при любых параметрах

    def timeout_for(self, params: P) -> int:
        """Предел попытки для этих параметров, не больше timeout_seconds.
        По умолчанию — timeout_seconds; Dispatcher прерывает run по нему."""

    @abstractmethod
    async def run(self, params: P, ctx: ToolContext) -> R:
        """Выполнить операцию.

        Получает уже проверенные параметры. Возвращает результат или
        выбрасывает ToolError. asyncio.CancelledError не перехватывает.
        """


class ToolError(Exception):
    """Отказ исполнителя: задание переходит в failed, ошибка сохраняется."""

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


class Registry:
    def register(self, tool: Tool[Any, Any]) -> None: ...   # повтор типа — ошибка при старте
    def get(self, type_: str) -> Tool[Any, Any]: ...        # неизвестный тип — KeyError
    def types(self) -> list[str]: ...
```

## 2. Что Tool получает

| Вход | Откуда | Гарантии |
|---|---|---|
| `params` | Параметры задания, сохранённые при создании | Уже прошли проверку по `params_model`: типы, диапазоны, формат URL. Это экземпляр модели, а не сырой JSON. |
| `ctx.job_id`, `ctx.attempt` | `Dispatcher` | Только для логов. Tool не меняет статус задания и не обращается к хранилищу. |
| `ctx.http` | `Dispatcher` | Единственный разрешённый способ сетевого запроса. Прямое использование HTTP-клиента в Tool запрещено. |

Tool **не получает** и не должен знать: пользователя, статус задания, ключ идемпотентности, очередь,
базу данных, канал событий.

## 3. Что Tool возвращает

- Экземпляр `result_model`. `Dispatcher` сериализует его в JSON, сохраняет в `Job.result` и переводит
  задание в `finished`.
- Результат описывает операцию целиком, включая частичные неудачи, если операция их допускает
  (например, недоступный URL в `fetch`). Частичная неудача — это данные результата, а не ошибка.
- Результат компактный: тела больших ответов и файлов в него не попадают, только извлечённые данные.

## 4. Как Tool отказывает

| Ситуация | Что делает Tool | Чем заканчивается задание |
|---|---|---|
| Операцию невозможно выполнить: файл больше лимита, ответ не HTML, сервер вернул ошибку для единственного URL | Выбрасывает `ToolError(code, message, details)` | `failed`, ошибка сохраняется в `Job.error` в формате `{code, message, details}`. Автоматического повтора нет: повторить может только пользователь через `retry`. |
| Сетевая ошибка после всех повторов `HttpAdapter` | Превращает ошибку адаптера в `ToolError` (или в элемент результата, если операция допускает частичные неудачи) | `failed` или `finished` — по описанию конкретного Tool ниже. |
| Непредвиденное исключение (ошибка в коде Tool) | Ничего не делает — исключение выходит из `run` | `failed` с кодом `internal_error`; подробности — только в логах, пользователю не раскрываются. |
| Превышен `timeout_for(params)` или воркер аварийно завершился | — | ARQ повторяет попытку внутри `running`; после исчерпания попыток — `failed` с кодом `timeout` или `worker_lost` ([ADR-0002](adr/0002-background-worker.md)). |
| Отмена задания | Получает `asyncio.CancelledError` в ближайшей точке `await`, освобождает ресурсы (`finally` / `async with`) и пробрасывает исключение дальше | `cancelled` ([job-lifecycle.md](job-lifecycle.md)). Перехватывать и «глушить» `CancelledError` запрещено. |

Коды ошибок — строки в `snake_case`, общие для всех Tool, если смысл совпадает: `http_status`,
`network_error`, `too_large`, `not_html`, `internal_error`, `timeout`, `worker_lost`.

## 5. Кто проверяет параметры

Параметры проверяет **вызывающий**, а не Tool:

1. При `POST /api/tasks` `TaskService` берёт Tool из `Registry` по типу и проверяет параметры по его
   `params_model`. Неизвестный тип или неверные параметры — `422` с указанием поля в `details.fields`
   ([api/openapi.yaml](api/openapi.yaml), схемы `*Params`); задание не создаётся
   и в очередь не попадает (requirements, «Ошибка 2»).
2. `Dispatcher` перед запуском восстанавливает модель из сохранённых параметров. Если это не удалось
   (например, схема изменилась между версиями), задание переходит в `failed` с кодом `invalid_params`,
   Tool не запускается.
3. Tool доверяет полученным параметрам и не перепроверяет их формат. Tool проверяет только то, что
   нельзя знать до выполнения: фактический размер файла, тип содержимого ответа, код ответа сервера.

Схема параметров объявлена в самом Tool, поэтому правила проверки и выполнения не расходятся, а `TaskService`
не перечисляет типы операций в своём коде.

## 6. Повторный запуск

Tool может быть запущен для одного задания несколько раз: автоматические попытки ARQ после сбоя воркера
и `retry` пользователя. Поэтому Tool обязан быть безопасным для повторного запуска: не иметь побочных
эффектов или делать их идемпотентными. Все четыре Tool первой версии только читают данные (ожидание,
HTTP GET) и этому требованию удовлетворяют.

## 7. Tool первой версии

Лимиты ниже — значения по умолчанию из конфигурации сервиса.

### `asleep` — ожидание

Эталонная длительная операция для проверки очереди, статусов и отмены.

| | |
|---|---|
| Параметры | `seconds: int`, от 1 до 3600 |
| Результат | `{"slept_seconds": 10}` |
| Отказы | Нет: операция всегда завершается, если её не отменили |
| Отмена | Прерывает ожидание сразу |
| `timeout_seconds` | 3630 (для `seconds = 3600`); `timeout_for` — `seconds + 30` |

### `fetch` — параллельный запрос списка URL

| | |
|---|---|
| Параметры | `urls: list[HttpUrl]`, от 1 до 20 адресов, без повторов |
| Результат | Список в порядке `urls`; для каждого URL — `{url, ok, status_code, content_type, body, error}`. `body` — JSON, если ответ в JSON, иначе текст, обрезанный до 64 КБ. `error` — `{code, message}` или `null`. |
| Частичные неудачи | Недоступный URL, таймаут, ответ `4xx`/`5xx` или ответ больше 1 МБ — это элемент результата с `ok: false` и заполненным `error`. Задание при этом `finished`, даже если ошибки по всем URL (requirements, сценарий 5). |
| Отказы | Только непредвиденная ошибка — `internal_error` |
| Отмена | Прерывает все незавершённые запросы |
| `timeout_seconds` | 120 |

Пример результата:

```json
[
  {"url": "https://api.example.com/a", "ok": true, "status_code": 200,
   "content_type": "application/json", "body": {"value": 1}, "error": null},
  {"url": "https://down.example.com", "ok": false, "status_code": null,
   "content_type": null, "body": null,
   "error": {"code": "network_error", "message": "Connection refused"}}
]
```

### `file_checksum` — SHA-256 файла по URL

| | |
|---|---|
| Параметры | `url: HttpUrl`; `max_bytes: int` — необязательный, не больше лимита сервиса 100 МБ |
| Результат | `{url, size_bytes, sha256, content_type}` |
| Отказы | `too_large` — размер по заголовку или фактически прочитанный объём превысил `max_bytes`, загрузка прерывается; `http_status` — сервер ответил не `2xx`; `network_error` — соединение оборвалось после повторов адаптера |
| Отмена | Прерывает потоковую загрузку, соединение закрывается |
| `timeout_seconds` | 300 |

Файл читается потоком и не сохраняется: хеш считается по мере загрузки.

### `page_digest` — сводка веб-страницы

| | |
|---|---|
| Параметры | `url: HttpUrl` |
| Результат | `{url, final_url, title, description, lang, words, links}`; отсутствующие на странице поля — `null` |
| Отказы | `not_html` — `Content-Type` ответа не `text/html`; `too_large` — страница больше 5 МБ; `http_status`, `network_error` — как у `file_checksum` |
| Отмена | Прерывает загрузку |
| `timeout_seconds` | 60 |

Во втором семестре извлечение сводки может быть заменено на краткое содержание от LLM без изменения
контракта: меняется только реализация `run` и, возможно, `result_model` (README, «Подключение LLM»).

## 8. Как добавить новый Tool

1. Описать `params_model` и `result_model`.
2. Реализовать класс-наследник `Tool` с методом `run`, внешние запросы — только через `ctx.http`.
3. Зарегистрировать экземпляр в `Registry`.

`Dispatcher`, `TaskService` и API не меняются: новый тип сразу принимается в `POST /api/tasks`
и проверяется по своей схеме.
