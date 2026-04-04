"""
SaaS backend: ИНН → реквизиты компании (DaData).

Публичное API: POST /company-by-inn (заголовок X-API-KEY).

Вариант C (без виджета amo): POST /integrations/amo/webhook — JSON с lead_id или типовой
формат вебхука amo (leads.add и т.д.); нужны AMOCRM_API_BASE, AMOCRM_ACCESS_TOKEN, DADATA_API_KEY,
поле ИНН на сделке (AMO_FIELD_INN или значение по умолчанию в коде).

Деплой на Render:
  - Environment (см. блок «Внешние API» ниже в коде)
  - Render подставляет PORT; для локали: uvicorn main:app --host 0.0.0.0 --port 10000
  - Start Command: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

import json
import logging
import os
import re
from urllib.parse import parse_qs
from datetime import datetime, timezone
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

# amoCRM — куда вставлять то, что выдаёт amo при создании виджета / интеграции
#
# 1) Только на сервере (Render → Environment — те же имена; не коммитить в git):
#    AMOCRM_API_BASE      — URL аккаунта: https://ВАШ_ПОДДОМЕН.amocrm.ru (без / в конце)
#    AMOCRM_ACCESS_TOKEN  — «долгоживущий ключ» / access token из блока «Ключи и доступы»
#                            интеграции (нужен, если этот бэкенд будет вызывать REST API amo)
#    AMOCRM_CLIENT_ID     — Client ID интеграции (если amo выдал; для OAuth с бэкенда)
#    AMOCRM_CLIENT_SECRET — Secret интеграции (только сюда или в env, не в JS виджета)
#
# 2) В коде виджета (архив .zip, script.js / manifest — в кабинете amo, НЕ в этом репозитории):
#    — URL вашего API: https://inn-efz1.onrender.com/company-by-inn
#    — Заголовок X-API-KEY: значение из словаря API_KEYS ниже (например test_key_1) —
#      это ВАШ ключ к бэкенду, его amo не выдаёт; придумываете вы и прописываете в виджете.
#
# 3) Секрет интеграции amo не вставляйте в публичный JS виджета — только env на Render или
#    серверную часть; иначе любой сможет прочитать ключ в исходниках виджета.
# ---------------------------------------------------------------------------
AMOCRM_API_BASE = os.environ.get("AMOCRM_API_BASE", "").strip().rstrip("/")
AMOCRM_ACCESS_TOKEN = os.environ.get("AMOCRM_ACCESS_TOKEN", "").strip()


def _env_amocrm_base() -> str:
    return os.environ.get("AMOCRM_API_BASE", "").strip().rstrip("/")


def _env_amocrm_token() -> str:
    """Токен из env; снимаем оборачивающие кавычки (часто копируют в Render с \"...\")."""
    t = os.environ.get("AMOCRM_ACCESS_TOKEN", "").strip()
    if len(t) >= 2 and t[0] == t[-1] and t[0] in "\"'":
        t = t[1:-1].strip()
    return t


def _amo_non_json_error_detail(response: httpx.Response) -> str:
    """Пояснение, если вместо JSON пришла HTML-страница или мусор."""
    ct = (response.headers.get("content-type") or "").lower()
    body = (response.text or "")[:400].lower()
    st = response.status_code
    if st == 204 or not (response.text or "").strip():
        return (
            f"amo вернул пустой ответ (HTTP {st}) для GET сделки. "
            "Сервер повторяет запрос через filter[id][]; если ошибка остаётся — проверьте id сделки, "
            "токен и доступ интеграции к сделкам."
        )
    base = (
        f"amo вернул не JSON (HTTP {st}). Проверьте AMOCRM_API_BASE и долгосрочный токен "
        f"из той же интеграции и аккаунта, что и сделки (например https://shabuninaleksei.amocrm.ru)."
    )
    if "text/html" in ct or "<!doctype" in body or "<html" in body:
        return (
            base
            + " Сейчас в ответе похоже HTML (часто неверный URL, истёкший токен или лишние кавычки "
            "вокруг значения AMOCRM_ACCESS_TOKEN на Render)."
        )
    return base


AMOCRM_CLIENT_ID = os.environ.get("AMOCRM_CLIENT_ID", "").strip()
AMOCRM_CLIENT_SECRET = os.environ.get("AMOCRM_CLIENT_SECRET", "").strip()
# Вебхук (вариант C): если задан — проверяем телом secret= или заголовок X-Webhook-Secret
AMO_WEBHOOK_SECRET = os.environ.get("AMO_WEBHOOK_SECRET", "").strip()

# ID полей сделки в amo → env можно переопределить; иначе значения из вашей вёрстки
_DEFAULT_AMO_LEAD_FIELDS: dict[str, str] = {
    "AMO_FIELD_INN": "1303797",
    "AMO_FIELD_KPP": "1303799",
    "AMO_FIELD_ADDRESS": "1001617",
    "AMO_FIELD_BANK": "1303155",
    "AMO_FIELD_RS": "1303807",
    "AMO_FIELD_CORR": "1303805",
}


def _amo_lead_field_id(env_name: str) -> int | None:
    v = os.environ.get(env_name, _DEFAULT_AMO_LEAD_FIELDS.get(env_name, "")).strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _dadata_row_to_amo_lead_cfv(row: dict[str, str]) -> list[dict[str, Any]]:
    """Собирает custom_fields_values для PATCH сделки."""
    mapping: list[tuple[str, str]] = [
        ("inn", "AMO_FIELD_INN"),
        ("kpp", "AMO_FIELD_KPP"),
        ("ogrn", "AMO_FIELD_OGRN"),
        ("address", "AMO_FIELD_ADDRESS"),
        ("bank_name", "AMO_FIELD_BANK"),
        ("settlement_account", "AMO_FIELD_RS"),
        ("corr_account", "AMO_FIELD_CORR"),
        ("name", "AMO_FIELD_COMPANY_NAME"),
        ("director", "AMO_FIELD_DIRECTOR"),
        ("status", "AMO_FIELD_STATUS"),
        ("okpo", "AMO_FIELD_OKPO"),
        ("okved", "AMO_FIELD_OKVED"),
        ("registration_date", "AMO_FIELD_REGISTRATION_DATE"),
        ("opf", "AMO_FIELD_OPF"),
        ("bic", "AMO_FIELD_BIC"),
    ]
    out: list[dict[str, Any]] = []
    for row_key, env_name in mapping:
        fid = _amo_lead_field_id(env_name)
        if fid is None:
            continue
        val = (row.get(row_key) or "").strip()
        if not val:
            continue
        out.append({"field_id": fid, "values": [{"value": val}]})
    return out


def _amo_company_inn_field_id() -> int | None:
    """Поле ИНН у компании (id часто отличается от поля на сделке)."""
    raw = os.environ.get("AMO_FIELD_INN_COMPANY", "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            return None
    return _amo_lead_field_id("AMO_FIELD_INN")


def _amo_linked_company_ids(lead: dict[str, Any]) -> list[int]:
    out: list[int] = []
    emb = lead.get("_embedded")
    if not isinstance(emb, dict):
        return out
    for c in emb.get("companies") or []:
        if not isinstance(c, dict):
            continue
        cid = c.get("id")
        if cid is None:
            continue
        try:
            out.append(int(cid))
        except (TypeError, ValueError):
            continue
    return out


def _flatten_amo_cfv_value_cell(val: Any) -> list[str]:
    """Все текстовые фрагменты из ячейки custom_fields_values[].values[] (в т.ч. вложенные dict/list в amo)."""
    out: list[str] = []
    if val is None:
        return out
    if isinstance(val, bool):
        return out
    if isinstance(val, (str, int, float)):
        out.append(str(val))
        return out
    if isinstance(val, dict):
        for x in val.values():
            out.extend(_flatten_amo_cfv_value_cell(x))
    elif isinstance(val, list):
        for x in val:
            out.extend(_flatten_amo_cfv_value_cell(x))
    return out


def _first_inn_substring_from_digits(digits: str) -> str:
    """10 или 12 подряд цифр; при более длинной строке — первое окно длины 12, затем 10."""
    if len(digits) in (10, 12):
        return digits
    for ln in (12, 10):
        if len(digits) < ln:
            continue
        for i in range(0, len(digits) - ln + 1):
            return digits[i : i + ln]
    return ""


def _scan_entity_custom_fields_for_inn_digits(entity: dict[str, Any]) -> str:
    """
    Ищем ИНН в кастомных полях: любая вложенность в values[], затем 10/12 цифр подряд.
    """
    cfv = entity.get("custom_fields_values")
    if not isinstance(cfv, list):
        return ""
    for block in cfv:
        if not isinstance(block, dict):
            continue
        for cell in block.get("values") or []:
            for text in _flatten_amo_cfv_value_cell(cell):
                digits = "".join(c for c in text if c.isdigit())
                inn = _first_inn_substring_from_digits(digits)
                if inn:
                    return inn
    return ""


def _inn_from_entity_name(entity: dict[str, Any]) -> str:
    name = entity.get("name")
    if name is None or not str(name).strip():
        return ""
    digits = "".join(c for c in str(name) if c.isdigit())
    return _first_inn_substring_from_digits(digits)


async def _amo_fetch_company_dict(amo: httpx.AsyncClient, company_id: int) -> dict[str, Any] | None:
    try:
        r = await amo.get(f"/api/v4/companies/{company_id}")
        r.raise_for_status()
    except httpx.HTTPStatusError:
        return None
    except httpx.RequestError:
        return None
    if r.status_code == 204 or not (r.text or "").strip():
        return None
    try:
        data = r.json()
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


async def _resolve_inn_for_webhook(
    amo: httpx.AsyncClient,
    lead: dict[str, Any],
    lead_inn_fid: int,
) -> str:
    """ИНН со сделки или со связанной компании (_embedded.companies)."""
    inn = _inn_from_lead_payload(lead, lead_inn_fid)
    if len(inn) in (10, 12):
        return inn
    inn = _scan_entity_custom_fields_for_inn_digits(lead)
    if len(inn) in (10, 12):
        logger.info("amo: ИНН на сделке найден по разбору кастомных полей (10/12 цифр)")
        return inn
    inn = _inn_from_entity_name(lead)
    if len(inn) in (10, 12):
        logger.info("amo: ИНН из названия сделки")
        return inn

    comp_fid = _amo_company_inn_field_id()
    if comp_fid is None:
        return _inn_from_lead_payload(lead, lead_inn_fid)
    for cid in _amo_linked_company_ids(lead):
        comp = await _amo_fetch_company_dict(amo, cid)
        if comp is None:
            continue
        inn = _inn_from_lead_payload(comp, comp_fid)
        if len(inn) in (10, 12):
            logger.info("amo: ИНН взят с компании id=%s (field_id=%s)", cid, comp_fid)
            return inn
        inn = _scan_entity_custom_fields_for_inn_digits(comp)
        if len(inn) in (10, 12):
            logger.info("amo: ИНН на компании id=%s найден по разбору полей (10/12 цифр)", cid)
            return inn
        inn = _inn_from_entity_name(comp)
        if len(inn) in (10, 12):
            logger.info("amo: ИНН из названия компании id=%s", cid)
            return inn
        cfv_dbg = comp.get("custom_fields_values")
        cfv_n = len(cfv_dbg) if isinstance(cfv_dbg, list) else "null"
        logger.warning("amo: компания id=%s — ИНН не извлечён (custom_fields_values len=%s)", cid, cfv_n)
    return _inn_from_lead_payload(lead, lead_inn_fid)


async def _amo_enrich_lead_with_companies_if_needed(
    amo: httpx.AsyncClient,
    lead: dict[str, Any],
    lead_id: int,
) -> dict[str, Any]:
    """
    Список сделок (filter/query) часто отдаёт карточку без _embedded.companies.
    Тогда один запрос GET с with=companies подтягивает связи для чтения ИНН с компании.
    """
    if _amo_linked_company_ids(lead):
        return lead
    try:
        r = await amo.get(f"/api/v4/leads/{lead_id}", params={"with": "companies"})
        r.raise_for_status()
    except (httpx.HTTPStatusError, httpx.RequestError) as e:
        logger.warning("amo GET lead %s with=companies: %s", lead_id, e)
        return lead
    if r.status_code == 204 or not (r.text or "").strip():
        return lead
    try:
        data = r.json()
    except json.JSONDecodeError:
        return lead
    if isinstance(data, dict) and _amo_linked_company_ids(data):
        logger.info("amo: для сделки %s подгружен _embedded.companies", lead_id)
        return data
    return lead


def _inn_from_lead_payload(lead: dict[str, Any], field_id: int) -> str:
    cfv = lead.get("custom_fields_values")
    if not isinstance(cfv, list):
        return ""
    for block in cfv:
        if not isinstance(block, dict):
            continue
        try:
            fid = int(block.get("field_id") or 0)
        except (TypeError, ValueError):
            continue
        if fid != field_id:
            continue
        for v in block.get("values") or []:
            if not isinstance(v, dict):
                continue
            raw = v.get("value")
            if raw is None:
                continue
            digits = "".join(c for c in str(raw) if c.isdigit())
            return digits
    return ""


def _normalize_amo_single_lead_json(raw: Any) -> dict[str, Any] | None:
    """GET /api/v4/leads/{id} обычно отдаёт объект сделки; иногда обёртка _embedded.leads."""
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return raw[0]
    if not isinstance(raw, dict):
        return None
    if "custom_fields_values" in raw or "id" in raw:
        return raw
    emb = raw.get("_embedded")
    if isinstance(emb, dict):
        leads = emb.get("leads")
        if isinstance(leads, list) and leads and isinstance(leads[0], dict):
            return leads[0]
    return raw


def _amo_embedded_leads(data: dict[str, Any]) -> list[dict[str, Any]]:
    emb = data.get("_embedded")
    if not isinstance(emb, dict):
        return []
    ls = emb.get("leads")
    if not isinstance(ls, list):
        return []
    return [x for x in ls if isinstance(x, dict)]


def _amo_find_lead_in_list_payload(data: dict[str, Any], lead_id: int) -> dict[str, Any] | None:
    for row in _amo_embedded_leads(data):
        rid = row.get("id")
        if rid is None:
            continue
        try:
            if int(rid) == int(lead_id):
                return row
        except (TypeError, ValueError):
            continue
    return None


async def _amo_fetch_lead_raw(amo: httpx.AsyncClient, lead_id: int) -> dict[str, Any]:
    """
    amo: GET /api/v4/leads/{id} — при отсутствии сделки официально отдаёт 204 (без тела).
    Далее: filter[id][], затем query= (поиск по полям, подхватит название «Сделка #…»).
    """
    lr = await amo.get(f"/api/v4/leads/{lead_id}", params={"with": "companies"})
    lr.raise_for_status()

    if lr.status_code != 204 and (lr.text or "").strip():
        try:
            one = lr.json()
            if isinstance(one, dict) and one.get("id") is not None:
                try:
                    if int(one["id"]) == int(lead_id):
                        return one
                except (TypeError, ValueError):
                    pass
        except json.JSONDecodeError:
            logger.warning("amo GET /api/v4/leads/%s: тело не JSON", lead_id)

    logger.info("amo: filter[id][] для сделки %s", lead_id)
    lr2 = await amo.get(
        "/api/v4/leads",
        params=[("filter[id][]", str(lead_id)), ("limit", "1")],
    )
    lr2.raise_for_status()
    if (lr2.text or "").strip():
        try:
            data2 = lr2.json()
            if isinstance(data2, dict):
                found = _amo_find_lead_in_list_payload(data2, lead_id)
                if found:
                    return found
        except json.JSONDecodeError:
            logger.warning("amo GET leads filter: не JSON")

    logger.info("amo: query=%s (поиск по полям сделки)", lead_id)
    lr3 = await amo.get("/api/v4/leads", params={"query": str(lead_id), "limit": 50})
    lr3.raise_for_status()
    if (lr3.text or "").strip():
        try:
            data3 = lr3.json()
            if isinstance(data3, dict):
                found = _amo_find_lead_in_list_payload(data3, lead_id)
                if found:
                    return found
        except json.JSONDecodeError:
            logger.warning("amo GET leads query: не JSON")

    raise HTTPException(
        status_code=502,
        detail=(
            f"Сделку {lead_id} API amo не вернуло. По документации amo ответ 204 на GET /api/v4/leads/{{id}} "
            "означает, что сделки с таким ID нет для этого токена. Проверьте: "
            "1) откройте карточку сделки и посмотрите id в URL (не только # в названии); "
            "2) amo → ваша интеграция → выданные доступы — должно быть право на чтение сделок; "
            "3) долгосрочный токен из этого же аккаунта (поддомен в AMOCRM_API_BASE)."
        ),
    )


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
    token = _resolve_dadata_api_key()
    secret = os.environ.get("DADATA_SECRET_KEY", "").strip() or None
    amo_base = _env_amocrm_base()
    amo_tok = _env_amocrm_token()
    amo_client: httpx.AsyncClient | None = None
    if amo_base and amo_tok:
        # Как в официальном PHP-клиенте (amocrm-api-php): User-Agent и X-Client-UUID с ID интеграции.
        # Без X-Client-UUID часть аккаунтов отдаёт урезанные ответы (см. getBaseHeaders в AmoCRMApiRequest).
        amo_headers: dict[str, str] = {
            "Authorization": f"Bearer {amo_tok}",
            "Accept": "application/json",
            "User-Agent": "inn-dadata-backend/1.0",
        }
        amo_client_uuid = os.environ.get("AMOCRM_CLIENT_ID", "").strip()
        if amo_client_uuid:
            amo_headers["X-Client-UUID"] = amo_client_uuid
        amo_client = httpx.AsyncClient(
            base_url=amo_base,
            headers=amo_headers,
            timeout=httpx.Timeout(30.0, connect=10.0),
            follow_redirects=False,
        )

    if token:
        async with DadataAsync(token, secret=secret, timeout=30) as dadata:
            app.state.dadata = dadata
            app.state.amo_http = amo_client
            try:
                yield
            finally:
                if amo_client is not None:
                    await amo_client.aclose()
    else:
        app.state.dadata = None
        app.state.amo_http = amo_client
        try:
            yield
        finally:
            if amo_client is not None:
                await amo_client.aclose()


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


def _parse_positive_int_id(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int) and v > 0:
        return v
    if isinstance(v, str) and v.strip().isdigit():
        n = int(v.strip())
        return n if n > 0 else None
    return None


def _lead_id_from_leads_sublist(block: Any) -> int | None:
    if not isinstance(block, list) or not block:
        return None
    first = block[0]
    if isinstance(first, dict):
        return _parse_positive_int_id(first.get("id"))
    return None


def extract_lead_id_from_amo_webhook_payload(data: Any) -> int | None:
    """
    ID сделки: кастомный JSON {"lead_id": N} или типовой вебхук amo
    (leads.add / update / delete / status — первый элемент).
    """
    if not isinstance(data, dict):
        return None
    for key in ("lead_id", "id", "leadId"):
        lid = _parse_positive_int_id(data.get(key))
        if lid is not None:
            return lid
    leads = data.get("leads")
    if isinstance(leads, dict):
        for sub in ("add", "update", "delete", "status"):
            lid = _lead_id_from_leads_sublist(leads.get(sub))
            if lid is not None:
                return lid
    return None


def extract_secret_from_amo_webhook_payload(data: Any) -> str | None:
    if not isinstance(data, dict):
        return None
    s = data.get("secret")
    if s is None:
        return None
    return str(s)


def _recover_lead_id_from_broken_body(text: str) -> dict[str, Any] | None:
    """
    amo иногда шлёт битый JSON: плейсхолдер не подставился, лишние кавычки и т.п.
    Тогда json.loads падает около «column 13» сразу после "lead_id":
    """
    for pat in (
        r'"lead_id"\s*:\s*(\d{1,15})\b',
        r"'lead_id'\s*:\s*(\d{1,15})\b",
        r'"lead_id"\s*:\s*"(\d{1,15})"',
    ):
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return {"lead_id": int(m.group(1))}
    return None


def _parse_amo_webhook_json_body(raw: bytes, content_type: str) -> Any | None:
    if not raw or not raw.strip():
        return {}
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    ct = (content_type or "").lower()

    if "application/x-www-form-urlencoded" in ct:
        qs = parse_qs(text, keep_blank_values=True)
        lid = (qs.get("lead_id") or qs.get("id") or [None])[0]
        if lid and str(lid).strip().isdigit():
            return {"lead_id": int(str(lid).strip())}
        logger.warning("amo webhook: form-urlencoded без числового lead_id")
        return None

    if "application/json" not in ct and not text.startswith(("{", "[")):
        logger.warning("amo webhook: не JSON (Content-Type=%s, первые символы не {/[)", content_type)
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        recovered = _recover_lead_id_from_broken_body(text)
        if recovered:
            logger.warning("amo webhook: JSON невалиден (%s), восстановлен lead_id из текста", e)
            return recovered
        logger.warning("amo webhook: ошибка JSON: %s", e)
        return None


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


def _opf_text(data: dict[str, Any]) -> str:
    opf = data.get("opf")
    if isinstance(opf, dict):
        return str(opf.get("full") or opf.get("short") or "")
    return str(opf or "").strip()


def _okved_text(data: dict[str, Any]) -> str:
    for row in data.get("okveds") or []:
        if isinstance(row, dict) and row.get("main"):
            code, name = row.get("code"), row.get("name")
            parts = [p for p in (code, name) if p]
            if parts:
                return " ".join(parts)
    ov = data.get("okved")
    if isinstance(ov, dict):
        code, name = ov.get("code"), ov.get("name")
        parts = [p for p in (code, name) if p]
        return " ".join(parts) if parts else ""
    return str(ov or "").strip()


def _reg_date_str(state: dict[str, Any]) -> str:
    rd = state.get("registration_date")
    if rd is None:
        return ""
    if isinstance(rd, (int, float)):
        try:
            dt = datetime.fromtimestamp(rd / 1000.0, tz=timezone.utc)
            return dt.strftime("%d.%m.%Y")
        except (OSError, ValueError, OverflowError):
            return str(rd)
    return str(rd).strip()


def _finance_str(data: dict[str, Any], *keys: str) -> str:
    fin = data.get("finance")
    if not isinstance(fin, dict):
        return ""
    for k in keys:
        v = fin.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def party_to_company(data: dict[str, Any]) -> dict[str, str]:
    name = (data.get("name") or {}).get("full_with_opf") or ""
    address = (data.get("address") or {}).get("value") or ""
    management = data.get("management") or {}
    director = management.get("name") or ""
    state = data.get("state") or {}
    status = state.get("status") or ""
    bic_val = data.get("bic") or (data.get("bank") or {}).get("bic") or ""
    bank_name = ""
    if isinstance(data.get("bank"), dict):
        bank_name = str(data["bank"].get("name") or "").strip()
    # Р/с, к/с в типовой выдаче findById/party часто отсутствуют — пробуем finance.* при наличии.
    rs = _finance_str(data, "account", "payment_account", "rs", "rasschet")
    ks = _finance_str(data, "correspondent_account", "ks", "korr")
    if not bank_name:
        bank_name = _finance_str(data, "bank_name")
    return {
        "name": name,
        "inn": str(data.get("inn") or ""),
        "kpp": str(data.get("kpp") or ""),
        "ogrn": str(data.get("ogrn") or ""),
        "address": address,
        "director": director,
        "status": str(status),
        "okpo": str(data.get("okpo") or ""),
        "okved": _okved_text(data),
        "registration_date": _reg_date_str(state),
        "bic": str(bic_val).strip(),
        "opf": _opf_text(data),
        "bank_name": bank_name,
        "settlement_account": rs,
        "corr_account": ks,
    }


async def _party_company_for_inn(request: Request, inn: str) -> dict[str, str] | None:
    """
    Кеш или DaData → словарь реквизитов. None если организация не найдена.
    Не меняет счётчики API_KEYS (для вебхука amo и внутренних вызовов).
    """
    if inn in CACHE:
        return CACHE[inn]

    dadata: DadataAsync | None = request.app.state.dadata
    if dadata is None:
        logger.error("DADATA_API_KEY отсутствовал при старте — перезапустите сервис")
        raise HTTPException(status_code=500, detail="Сервис временно недоступен")

    try:
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
        return None

    zeroth = suggestions[0]
    if not isinstance(zeroth, dict):
        logger.error("DaData find_by_id: первый элемент не объект")
        return None
    first = zeroth.get("data") or {}
    if not isinstance(first, dict):
        first = {}
    company = party_to_company(first)
    CACHE[inn] = company
    return company


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

    get_dadata_token()  # проверка, что ключ задан (и 500, если нет)
    company = await _party_company_for_inn(request, inn)
    if company is None:
        return JSONResponse(status_code=404, content={"error": "NOT_FOUND"})

    entry["used"] += 1
    return company


def _check_amo_webhook_secret(
    body_secret: str | None,
    header_secret: str | None,
) -> None:
    if not AMO_WEBHOOK_SECRET:
        return
    ok = body_secret == AMO_WEBHOOK_SECRET or (header_secret or "").strip() == AMO_WEBHOOK_SECRET
    if not ok:
        raise HTTPException(status_code=403, detail="Неверный секрет вебхука")


@app.get("/integrations/amo/webhook")
async def amo_webhook_get_info():
    """
    Подсказка: открытие URL в браузере шлёт GET — для работы нужен POST + JSON.
    Логи Render с «405» на этом пути чаще всего из‑за проверки ссылки в браузере.
    """
    return {
        "hint": "Этот адрес вызывается методом POST из amo или curl, не из адресной строки браузера.",
        "method": "POST",
        "content_type": "application/json",
        "example_body": {"lead_id": 12345},
    }


@app.post("/integrations/amo/webhook")
async def amo_sync_lead_webhook(
    request: Request,
    x_webhook_secret: Annotated[str | None, Header(alias="X-Webhook-Secret")] = None,
):
    """
    Вариант C: без виджета. URL для автоматизации amo «Отправить вебхук».

    Тело JSON (любой подходящий вариант):
      - {"lead_id": <id>} или {"id": <id>} — кастомный шаблон;
      - типовой вебхук amo с вложением leads.add[0].id / update / status и т.д.

    Если задан AMO_WEBHOOK_SECRET — передайте тот же текст в JSON \"secret\" или в заголовке X-Webhook-Secret.
    """
    raw = await request.body()
    ct = request.headers.get("content-type", "")
    data = _parse_amo_webhook_json_body(raw, ct)
    if data is None:
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "reason": "INVALID_JSON",
                "hint": (
                    "Тело должно быть валидным JSON. Подставьте числовой id сделки, без слов-заглушек: "
                    '{"lead_id": 12345678}'
                ),
            },
        )

    lead_id = extract_lead_id_from_amo_webhook_payload(data)
    body_secret = extract_secret_from_amo_webhook_payload(data)

    logger.info(
        "amo webhook: bytes=%s lead_id=%s top_keys=%s",
        len(raw),
        lead_id,
        list(data.keys()) if isinstance(data, dict) else type(data).__name__,
    )

    _check_amo_webhook_secret(body_secret, x_webhook_secret)

    if lead_id is None:
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "reason": "NO_LEAD_ID",
                "hint": "Добавьте в JSON lead_id или используйте шаблон с {{lead.id}} в поле lead_id",
            },
        )

    amo: httpx.AsyncClient | None = getattr(request.app.state, "amo_http", None)
    if amo is None:
        raise HTTPException(
            status_code=503,
            detail="amoCRM не настроен: задайте AMOCRM_API_BASE и AMOCRM_ACCESS_TOKEN",
        )

    inn_fid = _amo_lead_field_id("AMO_FIELD_INN")
    if inn_fid is None:
        raise HTTPException(status_code=500, detail="Не задано поле ИНН (AMO_FIELD_INN)")

    get_dadata_token()

    try:
        lead_raw = await _amo_fetch_lead_raw(amo, lead_id)
    except httpx.HTTPStatusError as e:
        if e.response is not None and 300 <= e.response.status_code < 400:
            loc = e.response.headers.get("location", "")[:200]
            logger.warning("amo GET lead %s: редирект %s → %s", lead_id, e.response.status_code, loc)
            raise HTTPException(
                status_code=502,
                detail=(
                    "amo ответил редиректом вместо JSON. Проверьте AMOCRM_API_BASE: "
                    "должен быть полный URL аккаунта, например https://ВАШ_ПОДДОМЕН.amocrm.ru без пути /api."
                ),
            ) from e
        logger.warning("amo GET lead %s: %s", lead_id, e)
        raise HTTPException(status_code=502, detail="Не удалось прочитать сделку в amo") from e
    except httpx.RequestError as e:
        logger.exception("amo недоступен: %s", e)
        raise HTTPException(status_code=502, detail="Ошибка сети при обращении к amo") from e

    lead = _normalize_amo_single_lead_json(lead_raw)
    if lead is None:
        logger.warning("amo GET lead %s: неожиданная форма ответа %s", lead_id, type(lead_raw).__name__)
        raise HTTPException(status_code=502, detail="Неожиданный ответ amo при чтении сделки")

    lead = await _amo_enrich_lead_with_companies_if_needed(amo, lead, lead_id)

    inn = await _resolve_inn_for_webhook(amo, lead, inn_fid)
    if len(inn) not in (10, 12):
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "reason": "BAD_INN",
                "lead_id": lead_id,
                "inn_digits": inn,
                "hint": "ИНН не на сделке и не в связанной компании, или задайте AMO_FIELD_INN_COMPANY (id поля ИНН у компании).",
            },
        )

    company = await _party_company_for_inn(request, inn)
    if company is None:
        return JSONResponse(
            status_code=200,
            content={"ok": False, "reason": "NOT_FOUND", "lead_id": lead_id, "inn": inn},
        )

    cfv = _dadata_row_to_amo_lead_cfv(company)
    if not cfv:
        return JSONResponse(
            status_code=200,
            content={"ok": False, "reason": "NO_MAPPED_FIELDS", "lead_id": lead_id},
        )

    patch_body = [{"id": lead_id, "custom_fields_values": cfv}]
    try:
        pr = await amo.patch("/api/v4/leads", json=patch_body)
        pr.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.warning("amo PATCH lead %s: %s %s", lead_id, e, e.response.text[:500] if e.response else "")
        raise HTTPException(status_code=502, detail="Не удалось обновить сделку в amo") from e
    except httpx.RequestError as e:
        logger.exception("amo недоступен при PATCH: %s", e)
        raise HTTPException(status_code=502, detail="Ошибка сети при обновлении amo") from e

    return {"ok": True, "lead_id": lead_id, "inn": inn, "fields_updated": len(cfv)}


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
