"""
SaaS backend: ИНН → реквизиты компании (DaData).

Деплой на Render:
  - Environment (см. блок «Внешние API» ниже в коде)
  - Render подставляет PORT; для локали: uvicorn main:app --host 0.0.0.0 --port 10000
  - Start Command: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated, Any

import httpx
from dadata import DadataAsync
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Внешние API — куда подключаться (ключи и URL: .env или переменные на Render)
#
# DaData (клиент https://github.com/hflabs/dadata-py):
#   DADATA_API_KEY — API-ключ в окружении (приоритет; так задают на Render)
#   DADATA_API_KEY_HERE — запасной вариант: ключ прямо в этом файле (только для локалки;
#     не публикуйте репозиторий с реальным значением)
#   DADATA_SECRET_KEY — секрет (опционально; нужен для части методов clean и т.п.)
#   База suggestions фиксирована в библиотеке (как в REST findById/party).
#
DADATA_API_KEY_HERE = ""

# amoCRM (REST v4, для будущих вызовов из этого сервиса):
#   AMOCRM_API_BASE — база без слэша на конце, например https://ВАШ_ПОДДОМЕН.amocrm.ru
#   AMOCRM_ACCESS_TOKEN — долгоживущий токен (если будете ходить в API с бэкенда)
# ---------------------------------------------------------------------------
AMOCRM_API_BASE = os.environ.get("AMOCRM_API_BASE", "").strip().rstrip("/")
AMOCRM_ACCESS_TOKEN = os.environ.get("AMOCRM_ACCESS_TOKEN", "").strip()

# Список API-ключей клиентов (лимиты и учёт вызовов)
API_KEYS: dict[str, dict[str, int]] = {
    "test_key_1": {
        "limit": 1000,
        "used": 0,
    },
}

# Кеш ответов по ИНН (in-memory; не шарится между воркерами)
CACHE: dict[str, dict[str, str]] = {}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _resolve_dadata_api_key() -> str:
    """Сначала env DADATA_API_KEY, иначе строка DADATA_API_KEY_HERE в этом файле."""
    return (os.environ.get("DADATA_API_KEY", "").strip() or DADATA_API_KEY_HERE.strip())


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Один DadataAsync на процесс (внутри — общий httpx.AsyncClient к suggestions API)
    token = _resolve_dadata_api_key()
    secret = os.environ.get("DADATA_SECRET_KEY", "").strip() or None
    if token:
        async with DadataAsync(token, secret=secret, timeout=30) as dadata:
            app.state.dadata = dadata
            yield
    else:
        app.state.dadata = None
        yield


app = FastAPI(title="INN → Company (DaData)", lifespan=lifespan)

# Разрешить все источники; с allow_origins=["*"] credentials в браузере должны быть false
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_to_400(request: Request, exc: RequestValidationError):  # noqa: ARG001
    """Ошибки тела запроса (в т.ч. пустой inn) → 400 с понятным detail."""
    errors = exc.errors()
    if not errors:
        return JSONResponse(status_code=400, content={"detail": "Некорректный запрос"})
    err = errors[0]
    loc = err.get("loc", ())
    if err.get("type") == "missing" and loc and loc[-1] == "inn":
        return JSONResponse(status_code=400, content={"detail": "Поле inn обязательно"})
    ctx = err.get("ctx") or {}
    inner = ctx.get("error")
    if isinstance(inner, Exception):
        return JSONResponse(status_code=400, content={"detail": str(inner)})
    msg = err.get("msg", "Некорректный запрос")
    if isinstance(msg, str) and msg.startswith("Value error, "):
        msg = msg.removeprefix("Value error, ")
    return JSONResponse(status_code=400, content={"detail": msg})


class InnBody(BaseModel):
    inn: str = Field(..., description="ИНН юрлица или ИП")

    @field_validator("inn")
    @classmethod
    def inn_rules(cls, v: str) -> str:
        s = str(v).strip()
        if not s:
            raise ValueError("Поле inn обязательно")
        if not s.isdigit():
            raise ValueError("ИНН должен содержать только цифры")
        if len(s) not in (10, 12):
            raise ValueError("Длина ИНН должна быть 10 или 12")
        return s


def get_dadata_token() -> str:
    key = _resolve_dadata_api_key()
    if not key:
        logger.error("DaData: не задан ни DADATA_API_KEY, ни DADATA_API_KEY_HERE")
        raise HTTPException(
            status_code=500,
            detail="Сервис временно недоступен",
        )
    return key


async def require_api_key(
    x_api_key: Annotated[str | None, Header(alias="X-API-KEY")] = None,
) -> str:
    if not x_api_key or not x_api_key.strip():
        raise HTTPException(status_code=403, detail="Требуется заголовок X-API-KEY")
    key = x_api_key.strip()
    if key not in API_KEYS:
        raise HTTPException(status_code=403, detail="Неверный API-ключ")
    return key


def limit_exceeded_response() -> JSONResponse:
    return JSONResponse(status_code=429, content={"error": "LIMIT_EXCEEDED"})


@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/stats")
async def stats(api_key: Annotated[str, Depends(require_api_key)]):
    entry = API_KEYS[api_key]
    return {"limit": entry["limit"], "used": entry["used"]}


def party_to_company(data: dict[str, Any]) -> dict[str, str]:
    name = (data.get("name") or {}).get("full_with_opf") or ""
    address = (data.get("address") or {}).get("value") or ""
    management = data.get("management") or {}
    director = management.get("name") or ""
    state = data.get("state") or {}
    status = state.get("status") or ""
    return {
        "name": name,
        "inn": str(data.get("inn") or ""),
        "kpp": str(data.get("kpp") or ""),
        "ogrn": str(data.get("ogrn") or ""),
        "address": address,
        "director": director,
        "status": str(status),
    }


@app.post("/company-by-inn")
async def company_by_inn(
    body: InnBody,
    api_key: Annotated[str, Depends(require_api_key)],
    request: Request,
):
    entry = API_KEYS[api_key]
    if entry["used"] >= entry["limit"]:
        return limit_exceeded_response()

    inn = body.inn

    if inn in CACHE:
        entry["used"] += 1
        return CACHE[inn]

    get_dadata_token()  # проверка, что ключ задан (и 500, если нет)
    dadata: DadataAsync | None = request.app.state.dadata
    if dadata is None:
        logger.error("DADATA_API_KEY отсутствовал при старте — перезапустите сервис")
        raise HTTPException(status_code=500, detail="Сервис временно недоступен")

    try:
        # Эквивалент: dadata.find_by_id("party", inn) из dadata-py
        suggestions = await dadata.find_by_id("party", inn)
    except httpx.HTTPStatusError as e:
        logger.exception("DaData HTTP error: %s", e)
        raise HTTPException(status_code=500, detail="Ошибка при обращении к DaData") from e
    except httpx.RequestError as e:
        logger.exception("DaData недоступна: %s", e)
        raise HTTPException(status_code=500, detail="Ошибка при обращении к DaData") from e
    except Exception as e:  # noqa: BLE001
        logger.exception("Неожиданная ошибка при запросе к DaData: %s", e)
        raise HTTPException(status_code=500, detail="Ошибка при обращении к DaData") from e

    suggestions = suggestions or []
    if not suggestions:
        return JSONResponse(status_code=404, content={"error": "NOT_FOUND"})

    first = suggestions[0].get("data") or {}
    company = party_to_company(first)
    CACHE[inn] = company
    entry["used"] += 1
    return company


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
