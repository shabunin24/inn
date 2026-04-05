"""
SaaS backend: ИНН → реквизиты компании (DaData).

Публичное API: POST /company-by-inn, POST /suggest-party (заголовок X-API-KEY).

POST /integrations/amo/webhook — робот amo (JSON с lead_id или типовой leads.status и т.д.) или
вызов из виджета с X-API-KEY. Форматы сущностей и полей — как в API v4 и в официальном клиенте:
https://github.com/amocrm/amocrm-api-php

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
from pathlib import Path
from urllib.parse import parse_qs
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal, NamedTuple

import httpx
from dadata import DadataAsync
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, field_validator

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
# 2) В клиентской части (ваш собранный пакет виджета / общий фронт интеграции — НЕ в этом репозитории):
#    — URL вашего API: https://inn-efz1.onrender.com/company-by-inn
#    — Заголовок X-API-KEY: значение из словаря API_KEYS ниже (например test_key_1) —
#      это ВАШ ключ к бэкенду; задаёте вы и подставляете туда, откуда грузится виджет.
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


def _amo_company_field_id(base_env: str) -> int | None:
    """Id поля у компании: AMO_FIELD_XXX_COMPANY, иначе то же, что для сделки."""
    raw = os.environ.get(f"{base_env}_COMPANY", "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            return None
    return _amo_lead_field_id(base_env)


# В amo у полей «число» value в API должен быть numeric; пустая строка даёт 400 InvalidType.
_AMO_CFV_NUMERIC_ROW_KEYS: frozenset[str] = frozenset(
    {
        "inn",
        "kpp",
        "ogrn",
        "settlement_account",
        "corr_account",
        "bic",
        "okpo",
    }
)


def _amo_cfv_values_cell(row_key: str, val: str, *, include_empty: bool) -> dict[str, Any] | None:
    """Одна ячейка values[0]; None — не включать поле в PATCH."""
    if not val:
        if not include_empty:
            return None
        if row_key in _AMO_CFV_NUMERIC_ROW_KEYS or row_key == "inn":
            return None
        return {"value": ""}
    if row_key in _AMO_CFV_NUMERIC_ROW_KEYS:
        compact = "".join(val.split())
        if compact.isdigit():
            return {"value": int(compact)}
    return {"value": val}


def _dadata_row_to_amo_cfv(
    row: dict[str, str],
    entity: Literal["lead", "company"],
    *,
    include_empty: bool = False,
) -> list[dict[str, Any]]:
    """
    Собирает custom_fields_values для PATCH сделки или компании.
    include_empty=True — шлём и пустые строки по всем настроенным field_id, чтобы в amo не оставались
    значения от предыдущей организации (иначе PATCH только «непустые» поля, остальное amo не трогает).
    """
    pick = _amo_lead_field_id if entity == "lead" else _amo_company_field_id
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
        fid = pick(env_name)
        if fid is None:
            continue
        val = (row.get(row_key) or "").strip()
        cell = _amo_cfv_values_cell(row_key, val, include_empty=include_empty)
        if cell is None:
            continue
        out.append({"field_id": fid, "values": [cell]})
    return out


def _amo_company_inn_field_id() -> int | None:
    """Поле ИНН у компании (id часто отличается от поля на сделке)."""
    return _amo_company_field_id("AMO_FIELD_INN")


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


class _InnWebhookResolution(NamedTuple):
    inn: str
    write_entity: Literal["lead", "company"]
    company_id: int | None


async def _resolve_inn_for_webhook(
    amo: httpx.AsyncClient,
    lead: dict[str, Any],
    lead_inn_fid: int,
    *,
    company_field_id: int | None = None,
) -> _InnWebhookResolution:
    """
    ИНН для DaData: сначала связанная компания (в карточке сделки ИНН обычно там;
    иначе после смены ИНН у компании вебхук брал бы старое значение со сделки или из скана полей).
    Потом — сделка.

    company_field_id — явный field_id ИНН у компании (из виджета field_inn_company или fallback);
    если None — как в env AMO_FIELD_INN_COMPANY / AMO_FIELD_INN.
    """
    comp_fid = company_field_id if company_field_id is not None else _amo_company_inn_field_id()
    if comp_fid is not None:
        for cid in _amo_linked_company_ids(lead):
            comp = await _amo_fetch_company_dict(amo, cid)
            if comp is None:
                continue
            inn = _inn_from_lead_payload(comp, comp_fid)
            if len(inn) in (10, 12):
                logger.info("amo: ИНН взят с компании id=%s (field_id=%s)", cid, comp_fid)
                return _InnWebhookResolution(inn, "company", cid)
            inn = _scan_entity_custom_fields_for_inn_digits(comp)
            if len(inn) in (10, 12):
                logger.info("amo: ИНН на компании id=%s найден по разбору полей (10/12 цифр)", cid)
                return _InnWebhookResolution(inn, "company", cid)
            inn = _inn_from_entity_name(comp)
            if len(inn) in (10, 12):
                logger.info("amo: ИНН из названия компании id=%s", cid)
                return _InnWebhookResolution(inn, "company", cid)
            cfv_dbg = comp.get("custom_fields_values")
            cfv_n = len(cfv_dbg) if isinstance(cfv_dbg, list) else "null"
            logger.warning("amo: компания id=%s — ИНН не извлечён (custom_fields_values len=%s)", cid, cfv_n)

    inn = _inn_from_lead_payload(lead, lead_inn_fid)
    if len(inn) in (10, 12):
        return _InnWebhookResolution(inn, "lead", None)
    inn = _scan_entity_custom_fields_for_inn_digits(lead)
    if len(inn) in (10, 12):
        logger.info("amo: ИНН на сделке найден по разбору кастомных полей (10/12 цифр)")
        return _InnWebhookResolution(inn, "lead", None)
    inn = _inn_from_entity_name(lead)
    if len(inn) in (10, 12):
        logger.info("amo: ИНН из названия сделки")
        return _InnWebhookResolution(inn, "lead", None)

    inn = _inn_from_lead_payload(lead, lead_inn_fid)
    return _InnWebhookResolution(inn, "lead", None)


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
# Смена версии сбрасывает кэш party по ИНН (например после изменения разбора названия из DaData).
_PARTY_CACHE_VER = "v4"


def _party_cache_key(inn: str) -> str:
    return f"{_PARTY_CACHE_VER}:{inn}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_AGENT_DEBUG_LOG = Path(__file__).resolve().parent / ".cursor" / "debug-b4b4e3.log"


def _agent_debug_ndjson(payload: dict[str, Any]) -> None:
    """Сессия отладки Cursor: NDJSON в репозитории (локальный uvicorn / доступный диск)."""
    rec = {
        "sessionId": "b4b4e3",
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        **payload,
    }
    try:
        _AGENT_DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
        with _AGENT_DEBUG_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _webhook_patch_include_empty_fields() -> bool:
    """Очищать в amo поля, для которых в DaData пусто (убирает «хвост» прошлой организации). Выключить: AMO_WEBHOOK_CLEAR_OLD_FIELDS=0"""
    return os.environ.get("AMO_WEBHOOK_CLEAR_OLD_FIELDS", "1").strip().lower() not in ("0", "false", "no")


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


class SuggestPartyBody(BaseModel):
    """Подсказки организаций по фрагменту ИНН или названию (DaData suggest party)."""

    query: str = Field(..., min_length=1, max_length=100)

    @field_validator("query")
    @classmethod
    def query_rules(cls, v: str) -> str:
        s = str(v).strip()
        if len(s) < 2:
            raise ValueError("Минимум 2 символа")
        return s[:100]


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
    ID сделки: кастомный JSON {"lead_id": N} или вебхук amo / digital pipeline:
    {"leads":{"status":[{id,...}]}} и др. (см. документацию digital pipeline webhooks).
    """
    if not isinstance(data, dict):
        return None
    for key in ("lead_id", "id", "leadId", "element_id", "entity_id"):
        lid = _parse_positive_int_id(data.get(key))
        if lid is not None:
            return lid
    inner = data.get("data")
    if isinstance(inner, dict):
        for key in ("id", "lead_id", "element_id"):
            lid = _parse_positive_int_id(inner.get(key))
            if lid is not None:
                return lid
    leads = data.get("leads")
    if isinstance(leads, dict):
        # Типовые ключи REST-вебхуков и digital pipeline (status, mail_in, call_in, …)
        for sub in (
            "add",
            "update",
            "delete",
            "status",
            "mail_in",
            "call_in",
            "chat",
            "site_visit",
            "period",
        ):
            lid = _lead_id_from_leads_sublist(leads.get(sub))
            if lid is not None:
                return lid
        for block in leads.values():
            lid = _lead_id_from_leads_sublist(block if isinstance(block, list) else None)
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
    if text.startswith("\ufeff"):
        text = text.lstrip("\ufeff")
    if not text:
        return {}
    ct = (content_type or "").lower()

    if "application/x-www-form-urlencoded" in ct:
        qs = parse_qs(text, keep_blank_values=True)
        for qk in ("lead_id", "id"):
            lid = (qs.get(qk) or [None])[0]
            if lid and str(lid).strip().isdigit():
                return {"lead_id": int(str(lid).strip())}
        for qk in ("data", "json", "body", "payload", "leads"):
            vals = qs.get(qk)
            if not vals or not (vals[0] or "").strip():
                continue
            blob = vals[0].strip()
            try:
                parsed = json.loads(blob)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed
        logger.warning("amo webhook: form-urlencoded без lead_id и без JSON в полях data/json/body")
        return None

    looks_json = text.startswith(("{", "["))
    if "application/json" not in ct and not looks_json:
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
    """Проверка живости и что задеплоена актуальная main (есть ли suggest-party)."""
    return {
        "status": "ok",
        "build": "suggest-party-v1",
        "post": [
            "/company-by-inn",
            "/suggest-party",
            "/api/suggest-party",
            "/integrations/amo/webhook",
        ],
    }


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


def _party_legal_name(data: dict[str, Any]) -> str:
    """Название из блока data.name; у ИП/филиалов часто пустой full_with_opf, но есть full/short."""
    n = data.get("name")
    if isinstance(n, dict):
        for k in ("full_with_opf", "full", "short"):
            v = n.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""
    if isinstance(n, str) and n.strip():
        return n.strip()
    return ""


def party_to_company(data: dict[str, Any], fallback_display: str | None = None) -> dict[str, str]:
    name = _party_legal_name(data)
    if not name and fallback_display:
        name = str(fallback_display).strip()
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


async def _party_company_for_inn(
    request: Request,
    inn: str,
    *,
    use_cache: bool = True,
) -> dict[str, str] | None:
    """
    Кеш или DaData → словарь реквизитов. None если организация не найдена.
    Для вебхука use_cache=False — всегда свежий findById (иначе после смены ИНН возможны «старые» реквизиты).
    """
    ck = _party_cache_key(inn)
    if use_cache and ck in CACHE:
        return CACHE[ck]

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
    sug = zeroth.get("value")
    fb = str(sug).strip() if sug is not None and str(sug).strip() else None
    company = party_to_company(first, fallback_display=fb)
    if use_cache:
        CACHE[ck] = company
    return company


@app.post("/company-by-inn")
async def company_by_inn(request: Request):
    """JSON + заголовок X-API-KEY или form/json с x_api_key в теле (виджет self.crm_post через прокси amo)."""
    ct = (request.headers.get("content-type") or "").lower()
    header_key = (request.headers.get("X-API-KEY") or "").strip()
    api_key_final = header_key if header_key in API_KEYS else ""
    inn_raw: Any = None

    if "application/x-www-form-urlencoded" in ct or "multipart/form-data" in ct:
        form = await request.form()
        if not api_key_final:
            api_key_final = str(form.get("x_api_key") or form.get("api_key") or "").strip()
        inn_raw = form.get("inn")
    else:
        raw = await request.body()
        try:
            jd = json.loads(raw.decode("utf-8") or "{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            jd = {}
        if isinstance(jd, dict):
            if not api_key_final:
                api_key_final = str(jd.get("x_api_key") or jd.get("api_key") or "").strip()
            inn_raw = jd.get("inn")

    if api_key_final not in API_KEYS:
        raise HTTPException(status_code=403, detail="Требуется заголовок X-API-KEY или поле x_api_key в теле")

    try:
        inn = InnBody.model_validate({"inn": str(inn_raw if inn_raw is not None else "")}).inn
    except ValidationError as e:
        raise HTTPException(status_code=400, detail="Некорректное поле inn") from e

    entry = API_KEYS[api_key_final]
    if entry["used"] >= entry["limit"]:
        return limit_exceeded_response()

    get_dadata_token()  # проверка, что ключ задан (и 500, если нет)
    # Без кэша: после смены ИНН и деплоя нужны актуальные данные DaData, не старый in-memory ответ.
    company = await _party_company_for_inn(request, inn, use_cache=False)
    if company is None:
        return JSONResponse(status_code=404, content={"error": "NOT_FOUND"})

    entry["used"] += 1
    return company


@app.post("/suggest-party")
@app.post("/api/suggest-party")
async def suggest_party(request: Request):
    """
    Подсказки при наборе текста (виджет). Не тратит лимит API_KEYS — только проверка ключа;
    вызовы DaData suggest учитывайте в квоте DaData.
    Тело: JSON {query} или form; ключ в заголовке или x_api_key в теле (crm_post).
    """
    ct = (request.headers.get("content-type") or "").lower()
    header_key = (request.headers.get("X-API-KEY") or "").strip()
    api_key_final = header_key if header_key in API_KEYS else ""
    query_raw: Any = None

    if "application/x-www-form-urlencoded" in ct or "multipart/form-data" in ct:
        form = await request.form()
        if not api_key_final:
            api_key_final = str(form.get("x_api_key") or form.get("api_key") or "").strip()
        query_raw = form.get("query")
    else:
        raw = await request.body()
        try:
            jd = json.loads(raw.decode("utf-8") or "{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            jd = {}
        if isinstance(jd, dict):
            if not api_key_final:
                api_key_final = str(jd.get("x_api_key") or jd.get("api_key") or "").strip()
            query_raw = jd.get("query")

    if api_key_final not in API_KEYS:
        raise HTTPException(status_code=403, detail="Требуется заголовок X-API-KEY или поле x_api_key в теле")

    try:
        body = SuggestPartyBody.model_validate({"query": str(query_raw or "")})
    except ValidationError as e:
        raise HTTPException(status_code=400, detail="Некорректный query") from e

    dadata: DadataAsync | None = request.app.state.dadata
    if dadata is None:
        raise HTTPException(status_code=503, detail="DaData не настроена")

    get_dadata_token()
    try:
        raw = await dadata.suggest("party", body.query, count=10)
    except httpx.HTTPStatusError as e:
        logger.exception("DaData suggest party HTTP: %s", e)
        raise HTTPException(status_code=502, detail="Ошибка DaData") from e
    except httpx.RequestError as e:
        logger.exception("DaData suggest party сеть: %s", e)
        raise HTTPException(status_code=502, detail="DaData недоступна") from e
    except Exception as e:  # noqa: BLE001
        logger.exception("DaData suggest party: %s", e)
        raise HTTPException(status_code=502, detail="Ошибка DaData") from e

    suggestions: list[dict[str, str]] = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        data = item.get("data") if isinstance(item.get("data"), dict) else {}
        inn = str(data.get("inn") or "").strip()
        if len(inn) not in (10, 12):
            continue
        label = str(item.get("value") or "").strip() or inn
        suggestions.append({"inn": inn, "label": label})

    return {"suggestions": suggestions}


def _check_amo_webhook_secret(
    body_secret: str | None,
    header_secret: str | None,
) -> None:
    if not AMO_WEBHOOK_SECRET:
        return
    ok = body_secret == AMO_WEBHOOK_SECRET or (header_secret or "").strip() == AMO_WEBHOOK_SECRET
    if not ok:
        raise HTTPException(status_code=403, detail="Неверный секрет вебхука")


def _check_amo_webhook_auth(
    body_secret: str | None,
    header_secret: str | None,
    x_api_key: str | None,
    body_api_key: str | None = None,
) -> None:
    """Робот amo — secret; виджет — X-API-KEY в заголовке или x_api_key в JSON/form (crm_post через прокси)."""
    for cand in ((x_api_key or "").strip(), (body_api_key or "").strip()):
        if cand and cand in API_KEYS:
            return
    _check_amo_webhook_secret(body_secret, header_secret)


async def _load_webhook_payload_dict(request: Request) -> tuple[dict[str, Any] | None, int]:
    """JSON как у робота/curl; form-urlencoded — как у self.crm_post виджета (прокси amo)."""
    ct = (request.headers.get("content-type") or "").lower()
    if "application/x-www-form-urlencoded" in ct or "multipart/form-data" in ct:
        form = await request.form()
        data: dict[str, Any] = {}
        for fk, fv in form.multi_items():
            if hasattr(fv, "read"):
                continue
            data[str(fk)] = fv
        return (data if data else None), 0
    raw = await request.body()
    parsed = _parse_amo_webhook_json_body(raw, request.headers.get("content-type", ""))
    if isinstance(parsed, dict):
        return parsed, len(raw)
    return None, len(raw)


@app.get("/integrations/amo/webhook")
async def amo_webhook_get_info():
    """
    Подсказка: открытие URL в браузере шлёт GET — для работы нужен POST + JSON.
    Логи Render с «405» на этом пути чаще всего из‑за проверки ссылки в браузере.
    """
    return {
        "hint": "Ответ на / только значит «сервер жив». Сделки в amo сами не обновятся — нужен POST на этот путь из amo (робот) или curl.",
        "method": "POST",
        "content_type": "application/json",
        "example_body": {"lead_id": 12345},
        "amo_setup": [
            "amo → Сделки → настройки digital pipeline → этап → «API: отправить webhook».",
            "URL: https://ВАШ.onrender.com/integrations/amo/webhook — amo сам шлёт JSON вида {\"leads\":{\"status\":[{\"id\":...}]}}; парсер это понимает.",
            "Кастомное тело: {\"lead_id\": \"{{lead.id}}\"} если в интерфейсе есть подстановка.",
            "Если робот не настроен: виджет после «Заполнить» дополнительно дергает этот URL с X-API-KEY.",
            "Без робота и без виджета сервер из amo сам не вызывается — только curl для проверки.",
        ],
        "self_test": (
            'curl -sS -X POST "https://ВАШ.onrender.com/integrations/amo/webhook" '
            '-H "Content-Type: application/json" -d \'{"lead_id": 12345678}\''
        ),
        "render_logs": "Ищите «amo webhook:» и при успехе ответ с ok:true; при ok:false — reason (BAD_INN, NO_LEAD_ID, …).",
    }


@app.post("/integrations/amo/webhook")
async def amo_sync_lead_webhook(
    request: Request,
    x_webhook_secret: Annotated[str | None, Header(alias="X-Webhook-Secret")] = None,
    x_api_key: Annotated[str | None, Header(alias="X-API-KEY")] = None,
):
    """
    Синхронизация сделки по ИНН (DaData → PATCH amo).

    Источники вызова:
      - Робот amo «API: отправить webhook» / кастомный JSON с lead_id;
      - Виджет после заполнения полей — POST с заголовком X-API-KEY (как у /company-by-inn).

    Тело JSON:
      - {\"lead_id\": <id>} или {\"id\": <id>};
      - опционально field_inn, field_inn_company — id полей ИНН (как в настройках виджета), иначе только env AMO_FIELD_*;
      - вебхук amo: {\"leads\": {\"status\": [{\"id\": ...}]}} и аналоги (mail_in, update, …).

    Если задан AMO_WEBHOOK_SECRET — для вызовов без X-API-KEY: \"secret\" в JSON или X-Webhook-Secret.
    """
    data, raw_len = await _load_webhook_payload_dict(request)
    if data is None:
        logger.info("amo webhook result: INVALID_JSON")
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "reason": "INVALID_JSON",
                "hint": (
                    "Тело должно быть валидным JSON или form с lead_id. Подставьте числовой id сделки: "
                    '{"lead_id": 12345678}'
                ),
            },
        )

    lead_id = extract_lead_id_from_amo_webhook_payload(data)
    body_secret = extract_secret_from_amo_webhook_payload(data)

    logger.info(
        "amo webhook: bytes=%s lead_id=%s top_keys=%s",
        raw_len,
        lead_id,
        list(data.keys()) if isinstance(data, dict) else type(data).__name__,
    )

    body_api_key = None
    if isinstance(data, dict):
        bk = data.get("x_api_key") or data.get("api_key")
        if isinstance(bk, str) and bk.strip():
            body_api_key = bk.strip()
    _check_amo_webhook_auth(body_secret, x_webhook_secret, x_api_key, body_api_key)

    if lead_id is None:
        logger.info("amo webhook result: NO_LEAD_ID")
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

    field_inn_body = _parse_positive_int_id(data.get("field_inn")) if isinstance(data, dict) else None
    field_inn_company_body = _parse_positive_int_id(data.get("field_inn_company")) if isinstance(data, dict) else None
    inn_fid = field_inn_body or _amo_lead_field_id("AMO_FIELD_INN")
    if inn_fid is None:
        raise HTTPException(
            status_code=500,
            detail="Не задано поле ИНН: укажите AMO_FIELD_INN на сервере или передайте field_inn в JSON (как в виджете).",
        )
    # Как в виджете: компания — field_inn_company или тот же id, что и на сделке; иначе env.
    company_read_fid = field_inn_company_body or field_inn_body or _amo_company_inn_field_id()

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

    resolved = await _resolve_inn_for_webhook(
        amo,
        lead,
        inn_fid,
        company_field_id=company_read_fid,
    )
    inn = resolved.inn
    write_entity = resolved.write_entity
    write_company_id = resolved.company_id
    logger.info(
        "amo webhook: resolved inn=%s entity=%s company_id=%s lead_id=%s",
        inn,
        write_entity,
        write_company_id,
        lead_id,
    )
    if len(inn) not in (10, 12):
        logger.info("amo webhook result: BAD_INN lead_id=%s inn_digits=%r", lead_id, inn)
        _agent_debug_ndjson(
            {
                "location": "main.py:amo_sync_lead_webhook",
                "message": "BAD_INN",
                "hypothesisId": "H1",
                "data": {"lead_id": lead_id, "inn_len": len(inn), "write_entity_guess": write_entity},
            }
        )
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "reason": "BAD_INN",
                "lead_id": lead_id,
                "inn_digits": inn,
                "hint": (
                    "Нет ИНН 10/12 цифр: на новой сделке привяжите компанию с ИНН или заполните ИНН на сделке. "
                    "Если ИНН только у компании — в Render задайте AMO_FIELD_INN_COMPANY (id поля ИНН компании)."
                ),
            },
        )

    company = await _party_company_for_inn(request, inn, use_cache=False)
    if company is None:
        logger.info("amo webhook result: NOT_FOUND lead_id=%s inn=%s", lead_id, inn)
        _agent_debug_ndjson(
            {
                "location": "main.py:amo_sync_lead_webhook",
                "message": "NOT_FOUND",
                "hypothesisId": "H1",
                "data": {"lead_id": lead_id, "inn": inn},
            }
        )
        return JSONResponse(
            status_code=200,
            content={"ok": False, "reason": "NOT_FOUND", "lead_id": lead_id, "inn": inn},
        )

    cfv = _dadata_row_to_amo_cfv(
        company,
        write_entity,
        include_empty=_webhook_patch_include_empty_fields(),
    )
    # В карточке amo «название компании» — стандартное поле name в API, не кастомное (AMO_FIELD_COMPANY_NAME часто не задан).
    legal_name = (company.get("name") or "").strip() if write_entity == "company" else ""
    if not cfv and not (write_entity == "company" and legal_name):
        logger.info("amo webhook result: NO_MAPPED_FIELDS lead_id=%s", lead_id)
        _agent_debug_ndjson(
            {
                "location": "main.py:amo_sync_lead_webhook",
                "message": "NO_MAPPED_FIELDS",
                "hypothesisId": "H2",
                "data": {"lead_id": lead_id, "write_entity": write_entity},
            }
        )
        return JSONResponse(
            status_code=200,
            content={"ok": False, "reason": "NO_MAPPED_FIELDS", "lead_id": lead_id},
        )

    if write_entity == "company":
        if write_company_id is None:
            logger.error("amo webhook: write_entity=company без company_id, lead_id=%s", lead_id)
            logger.info("amo webhook result: INTERNAL_NO_COMPANY_ID lead_id=%s", lead_id)
            return JSONResponse(
                status_code=200,
                content={
                    "ok": False,
                    "reason": "INTERNAL_NO_COMPANY_ID",
                    "lead_id": lead_id,
                },
            )
        patch_url = "/api/v4/companies"
        patch_id = write_company_id
        patch_log = f"company {patch_id}"
    else:
        patch_url = "/api/v4/leads"
        patch_id = lead_id
        patch_log = f"lead {patch_id}"

    patch_item: dict[str, Any] = {"id": patch_id}
    if cfv:
        patch_item["custom_fields_values"] = cfv
    if write_entity == "company" and legal_name:
        # Лимит названия в amo обычно сотни символов; обрезаем по символам, не по байтам.
        patch_item["name"] = legal_name[:255]
    patch_body = [patch_item]
    logger.info(
        "amo webhook PATCH %s: name_len=%s cfv_count=%s keys=%s",
        patch_log,
        len(legal_name) if write_entity == "company" else 0,
        len(cfv),
        list(patch_item.keys()),
    )
    try:
        pr = await amo.patch(patch_url, json=patch_body)
        pr.raise_for_status()
    except httpx.HTTPStatusError as e:
        raw = (e.response.text or "")[:1500] if e.response else ""
        logger.warning("amo PATCH %s: %s body=%s", patch_log, e, raw[:800])
        raise HTTPException(
            status_code=502,
            detail={
                "message": f"Не удалось обновить {'компанию' if write_entity == 'company' else 'сделку'} в amo",
                "amo_http_status": e.response.status_code if e.response else None,
                "amo_response": raw or None,
                "hint": (
                    "Поля в PATCH должны существовать у той сущности (сделка или компания), куда идёт запрос. "
                    "При необходимости задайте AMO_FIELD_*_COMPANY для id полей компании."
                ),
            },
        ) from e
    except httpx.RequestError as e:
        logger.exception("amo недоступен при PATCH: %s", e)
        raise HTTPException(status_code=502, detail="Ошибка сети при обновлении amo") from e

    # ИНН часто хранится у связанной компании → PATCH шёл только в /companies; на карточке сделки
    # пользователь смотрит доп. поля сделки — дублируем реквизиты на lead теми же AMO_FIELD_* (сделка).
    lead_mirror_cfv_count = 0
    if write_entity == "company":
        lead_cfv = _dadata_row_to_amo_cfv(
            company,
            "lead",
            include_empty=_webhook_patch_include_empty_fields(),
        )
        if lead_cfv:
            try:
                pr_lead = await amo.patch(
                    "/api/v4/leads",
                    json=[{"id": lead_id, "custom_fields_values": lead_cfv}],
                )
                pr_lead.raise_for_status()
                lead_mirror_cfv_count = len(lead_cfv)
            except httpx.HTTPStatusError as e:
                raw_l = (e.response.text or "")[:800] if e.response else ""
                logger.warning(
                    "amo webhook: зеркалирование на сделку lead_id=%s не удалось: %s body=%s",
                    lead_id,
                    e,
                    raw_l,
                )
            except httpx.RequestError as e:
                logger.warning(
                    "amo webhook: сеть при зеркалировании на сделку lead_id=%s: %s",
                    lead_id,
                    e,
                )

    name_applied = bool(write_entity == "company" and legal_name)
    out: dict[str, Any] = {
        "ok": True,
        "lead_id": lead_id,
        "inn": inn,
        "fields_updated": len(cfv) + (1 if name_applied else 0) + lead_mirror_cfv_count,
        "updated_entity": write_entity,
    }
    if lead_mirror_cfv_count:
        out["lead_mirror_fields_updated"] = lead_mirror_cfv_count
    if name_applied:
        out["company_name_applied"] = True
        out["company_name_preview"] = legal_name[:120] + ("…" if len(legal_name) > 120 else "")
    if write_entity == "company" and write_company_id is not None:
        out["company_id"] = write_company_id
    _agent_debug_ndjson(
        {
            "location": "main.py:amo_sync_lead_webhook",
            "message": "OK",
            "hypothesisId": "H1",
            "data": {
                "lead_id": lead_id,
                "write_entity": write_entity,
                "company_id": write_company_id,
                "cfv_count": len(cfv),
                "lead_mirror_cfv_count": lead_mirror_cfv_count,
                "fields_updated": out["fields_updated"],
            },
        }
    )
    logger.info("amo webhook OK %s", out)
    return out


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
