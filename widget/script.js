/**
 * Виджет amoCRM: колбэки и окружение — по гайду «Интеграции / script.js» (CustomWidget, APP.data.current_card).
 * REST: те же пути и сущности, что в API v4 и в официальной PHP-библиотеке (leads, companies, custom_fields_values).
 *
 * @see https://www.amocrm.ru/developers/content/integrations/script_js
 * @see https://www.amocrm.ru/developers/content/integrations/areas (lcard, comcard, …)
 * @see https://www.amocrm.ru/developers/content/crm_platform/api-v4
 * @see https://github.com/amocrm/amocrm-api-php
 *
 * Внешний бэкенд: по доке amo — self.crm_post (прокси); при сбое — fetch на FastAPI (CORS *).
 */
define(['jquery'], function ($) {
  'use strict';

  /**
   * Поиск в DevTools (Sources / по всем файлам): INNDADATA_WIDGET_F12_MARKER — этот файл виджета.
   * Строку «render» не используйте: в коде amo есть onRender*, это не Render.com и не ваш бэкенд.
   * URL бэкенда в исходнике нет — его подставляет amo из настроек (backend_url) в рантайме.
   */
  var WIDGET_F12_SEARCH = 'INNDADATA_WIDGET_F12_MARKER';

  /**
   * Авторизованные запросы к /api/v4/* внутри аккаунта (типичный метод виджета в amo).
   */
  function hasAmoAuthorizedAjax(self) {
    return !!(self && typeof self.$authorizedAjax === 'function');
  }

  function amoApiV4(self, options) {
    if (!hasAmoAuthorizedAjax(self)) return null;
    return self.$authorizedAjax(options);
  }

  function langPack(self) {
    try {
      if (typeof APP !== 'undefined' && self.i18n) {
        return self.i18n(APP.lang_id) || {};
      }
    } catch (e) {
      /* ignore */
    }
    return {};
  }

  function tr(self, key, fallback) {
    var pack = langPack(self);
    if (pack[key]) return pack[key];
    if (key.indexOf('.') !== -1) {
      var parts = key.split('.');
      var o = pack;
      for (var i = 0; i < parts.length; i++) {
        o = o && o[parts[i]];
      }
      if (typeof o === 'string') return o;
    }
    return fallback;
  }

  function isDeveloperMode(settings) {
    if (!settings) return false;
    var v = String(settings.developer_mode || '')
      .trim()
      .toLowerCase();
    return v === '1' || v === 'true' || v === 'yes' || v === 'on';
  }

  /** Лог в консоль (F12) и в блок .js-inn-dadata-dev, если в настройках developer_mode = 1 */
  function devTrace(self, settings, label, data) {
    if (!isDeveloperMode(settings)) return;
    try {
      if (typeof console !== 'undefined' && console.warn) {
        console.warn('[INN→DaData]', label, data);
      }
    } catch (e0) {
      /* ignore */
    }
    try {
      var $d = $('.js-inn-dadata-dev');
      if ($d.length) {
        var line =
          new Date().toTimeString().slice(0, 8) +
          ' ' +
          label +
          (data !== undefined ? ' ' + JSON.stringify(data) : '') +
          '\n';
        var t = $d.text() + line;
        $d.text(t.length > 5000 ? t.slice(-5000) : t);
      }
    } catch (e1) {
      /* ignore */
    }
  }

  /**
   * Вызов вашего API с карточки amo.
   *
   * Документация amo (script.js): для кросс-доменных запросов нужен self.crm_post — прокси amoCRM;
   * «браузер может блокировать кросс-доменные запросы» при работе по SSL.
   * Поэтому порядок: 1) crm_post 2) при ошибке прокси или битом JSON — fetch (CORS на FastAPI = *).
   * Если наоборот сначала fetch — в части окружений запрос до Render не уходит (CSP/connect-src),
   * в «Сети» может не быть onrender, хотя curl с ПК работает.
   *
   * @see https://www.amocrm.ru/developers/content/integrations/script_js (crm_post)
   */
  function backendAmoProxyPost(self, settings, pathSuffix, jsonFields, onResult) {
    var base = String(settings.backend_url || '').trim().replace(/\/+$/, '');
    var key = String(settings.x_api_key || '').trim();
    var url = base + pathSuffix;
    if (!base || !key) {
      onResult({ ok: false, status: 0, body: null, err: 'no_base_or_key' });
      return;
    }

    function normalizeParsed(msg) {
      if (msg != null && typeof msg === 'string') {
        try {
          return JSON.parse(msg);
        } catch (e) {
          return { _parse_error: true, raw: msg };
        }
      }
      return msg;
    }

    function finish(ok, status, body, err) {
      var badParse = body && body._parse_error;
      onResult({
        ok: ok && !badParse,
        status: badParse ? 502 : status,
        body: body,
        err: badParse ? 'parse' : err,
      });
    }

    function runFetch() {
      if (typeof fetch !== 'function') {
        onResult({ ok: false, status: 0, body: null, err: 'network' });
        return;
      }
      fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-KEY': key,
        },
        body: JSON.stringify(jsonFields),
        cache: 'no-store',
        mode: 'cors',
        credentials: 'omit',
      })
        .then(function (res) {
          return res.text().then(function (text) {
            var b = null;
            try {
              b = text ? JSON.parse(text) : null;
            } catch (e) {
              b = { _parse_error: true, raw: text };
            }
            finish(res.ok, res.status, b, res.ok ? null : 'http');
          });
        })
        .catch(function () {
          onResult({ ok: false, status: 0, body: null, err: 'network' });
        });
    }

    var flat = { x_api_key: key };
    for (var fk in jsonFields) {
      if (Object.prototype.hasOwnProperty.call(jsonFields, fk)) flat[fk] = jsonFields[fk];
    }

    if (self && typeof self.crm_post === 'function') {
      self.crm_post(
        url,
        flat,
        function (msg) {
          var parsed = normalizeParsed(msg);
          if (parsed && parsed._parse_error) {
            runFetch();
            return;
          }
          finish(true, 200, parsed, null);
        },
        'json',
        function () {
          runFetch();
        },
      );
      return;
    }

    runFetch();
  }

  /** Стабильное сравнение карточки при переключении сделок (id в amo бывает string|number). */
  function cardContextKey(c) {
    return c && c.kind && c.id != null ? c.kind + ':' + String(c.id) : '';
  }

  /**
   * Карточка сделки (lcard) или компании (comcard): сначала self.system().area из документации amo,
   * затем запасные варианты (getWidgetsArea / getBaseEntity / модель).
   * @param self экземпляр виджета (this в callbacks); можно не передавать, но тогда хуже определяется тип карточки.
   */
  function currentCardContext(self) {
    if (typeof APP === 'undefined' || !APP.data || !APP.data.current_card) return null;
    var id = APP.data.current_card.id;
    if (id === 0 || id === '0') return null;
    var model = APP.data.current_card.model;
    if (!model) return null;

    var kind = null;
    if (self && typeof self.system === 'function') {
      try {
        var sysArea = String(self.system().area || '');
        if (sysArea === 'lcard' || sysArea.indexOf('lcard') === 0) kind = 'leads';
        else if (sysArea === 'comcard' || sysArea.indexOf('comcard') === 0) kind = 'companies';
        else if (sysArea === 'ccard' || sysArea.indexOf('ccard') === 0) return null;
      } catch (e0) {
        /* ignore */
      }
    }
    if (!kind && APP.getWidgetsArea) {
      var wa = APP.getWidgetsArea();
      var as = String(wa == null ? '' : wa);
      if (as === 'leads_card' || as.indexOf('lcard') !== -1) kind = 'leads';
      else if (as === 'companies_card' || as.indexOf('comcard') !== -1) kind = 'companies';
    }
    if (!kind && APP.isCard && APP.getBaseEntity) {
      var be = APP.getBaseEntity();
      if (be === 'companies') kind = 'companies';
      else if (be === 'leads') kind = 'leads';
    }
    if (!kind) {
      try {
        var mod = model.module || (model.get && model.get('element_type'));
        if (mod === 'leads' || mod === 'lead') kind = 'leads';
        else if (mod === 'companies' || mod === 'company') kind = 'companies';
      } catch (e1) {
        /* ignore */
      }
    }
    if (!kind) return null;
    return { kind: kind, id: id, model: model };
  }

  function extractInnFromCfv(cfv, fieldId) {
    if (!fieldId || !cfv || !cfv.length) return '';
    var row = null;
    for (var i = 0; i < cfv.length; i++) {
      if (Number(cfv[i].field_id) === Number(fieldId)) {
        row = cfv[i];
        break;
      }
    }
    if (!row || !row.values || !row.values.length) return '';
    var v = row.values[0].value;
    return String(v == null ? '' : v).replace(/\D/g, '');
  }

  function extractInnFromModel(model, fieldId) {
    if (!model || !fieldId) return '';
    return extractInnFromCfv(model.get('custom_fields_values'), fieldId);
  }

  /**
   * На сделке ИНН часто в блоке компании (_embedded.companies), а не в полях сделки.
   * patchMeta: если ИНН с компании — PATCH /companies/{id}, иначе PATCH сделки.
   */
  function resolveInnAndPatch(ctx, model, settings) {
    var leadFid = parseInt(String(settings.field_inn || '').trim(), 10);
    var compFidOnly = parseInt(String(settings.field_inn_company || '').trim(), 10);
    var compFid = compFidOnly || leadFid;
    if (!leadFid) leadFid = compFidOnly;
    if (!compFid) compFid = leadFid;
    if ((!leadFid && !compFid) || !model) return { inn: '', patchMeta: null };

    if (ctx.kind === 'companies') {
      var innC = extractInnFromCfv(model.get('custom_fields_values'), compFid);
      if (innC.length !== 10 && innC.length !== 12) innC = extractInnFromCfv(model.get('custom_fields_values'), leadFid);
      return { inn: innC, patchMeta: null };
    }

    var inn = extractInnFromCfv(model.get('custom_fields_values'), leadFid);
    if (inn.length === 10 || inn.length === 12) return { inn: inn, patchMeta: null };
    inn = extractInnFromCfv(model.get('custom_fields_values'), compFid);
    if (inn.length === 10 || inn.length === 12) return { inn: inn, patchMeta: null };

    var emb = model.get('_embedded');
    if (emb && emb.companies && emb.companies.length) {
      for (var j = 0; j < emb.companies.length; j++) {
        var co = emb.companies[j];
        if (!co) continue;
        inn = extractInnFromCfv(co.custom_fields_values, compFid);
        if (inn.length !== 10 && inn.length !== 12) inn = extractInnFromCfv(co.custom_fields_values, leadFid);
        if ((inn.length === 10 || inn.length === 12) && co.id) {
          return { inn: inn, patchMeta: { kind: 'companies', id: co.id } };
        }
      }
    }
    return { inn: '', patchMeta: null };
  }

  /** У карточки сделки в _embedded.companies часто нет custom_fields_values — только id. */
  function fetchCompanyInnFromAmoApi(self, model, settings, done) {
    var leadFid = parseInt(String(settings.field_inn || '').trim(), 10);
    var compFidOnly = parseInt(String(settings.field_inn_company || '').trim(), 10);
    var compFid = compFidOnly || leadFid;
    if (!leadFid) leadFid = compFidOnly;
    if (!compFid) compFid = leadFid;
    if ((!leadFid && !compFid) || !hasAmoAuthorizedAjax(self)) {
      done({ inn: '', patchMeta: null });
      return;
    }
    var emb = model.get('_embedded');
    if (!emb || !emb.companies || !emb.companies.length) {
      done({ inn: '', patchMeta: null });
      return;
    }
    var ids = [];
    for (var i = 0; i < emb.companies.length; i++) {
      if (emb.companies[i] && emb.companies[i].id) ids.push(emb.companies[i].id);
    }
    if (!ids.length) {
      done({ inn: '', patchMeta: null });
      return;
    }
    var tryIdx = 0;
    function tryNext() {
      if (tryIdx >= ids.length) {
        done({ inn: '', patchMeta: null });
        return;
      }
      var cid = ids[tryIdx++];
      var xhrCo = amoApiV4(self, {
        url: '/api/v4/companies/' + cid,
        method: 'GET',
      });
      if (!xhrCo) {
        tryNext();
        return;
      }
      xhrCo
        .done(function (data) {
          var cfv = (data && data.custom_fields_values) || [];
          var inn = extractInnFromCfv(cfv, compFid);
          if (inn.length !== 10 && inn.length !== 12) inn = extractInnFromCfv(cfv, leadFid);
          if (inn.length === 10 || inn.length === 12) {
            done({ inn: inn, patchMeta: { kind: 'companies', id: cid } });
          } else {
            tryNext();
          }
        })
        .fail(function () {
          tryNext();
        });
    }
    tryNext();
  }

  function resolveInnAndPatchWithApi(self, ctx, model, settings, done) {
    var sync = resolveInnAndPatch(ctx, model, settings);
    if (sync.inn.length === 10 || sync.inn.length === 12) {
      done(sync);
      return;
    }
    if (ctx.kind !== 'leads') {
      done(sync);
      return;
    }
    fetchCompanyInnFromAmoApi(self, model, settings, done);
  }

  /** Объект сделки из ответа amo как минимальная Backbone-модель (get). */
  function wrapLeadPayloadAsModel(payload) {
    return {
      get: function (key) {
        return payload ? payload[key] : undefined;
      },
    };
  }

  /**
   * Сделка: ИНН из актуального GET /leads/{id}?with=companies — в интерфейсе model.get часто отстаёт
   * от сохранённых данных, из‑за этого при смене ИНН автозаполнение не срабатывает.
   * При ошибке GET — запасной разбор по fallbackCtx/fallbackModel (карточка в памяти).
   */
  function resolveInnForLeadCard(self, leadId, settings, done, fallbackCtx, fallbackModel) {
    if (!leadId || !hasAmoAuthorizedAjax(self)) {
      if (fallbackCtx && fallbackModel) {
        resolveInnAndPatchWithApi(self, fallbackCtx, fallbackModel, settings, done);
      } else {
        done({ inn: '', patchMeta: null });
      }
      return;
    }
    var xhrLead = amoApiV4(self, {
      url: '/api/v4/leads/' + encodeURIComponent(String(leadId)),
      method: 'GET',
      data: { with: 'companies' },
    });
    if (!xhrLead) {
      if (fallbackCtx && fallbackModel) {
        resolveInnAndPatchWithApi(self, fallbackCtx, fallbackModel, settings, done);
      } else {
        done({ inn: '', patchMeta: null });
      }
      return;
    }
    xhrLead
      .done(function (payload) {
        if (!payload || payload.id == null) {
          if (fallbackCtx && fallbackModel) {
            resolveInnAndPatchWithApi(self, fallbackCtx, fallbackModel, settings, done);
          } else {
            done({ inn: '', patchMeta: null });
          }
          return;
        }
        var m = wrapLeadPayloadAsModel(payload);
        var ctxApi = { kind: 'leads', id: payload.id, model: m };
        var sync = resolveInnAndPatch(ctxApi, m, settings);
        if (sync.inn.length === 10 || sync.inn.length === 12) {
          done(sync);
          return;
        }
        fetchCompanyInnFromAmoApi(self, m, settings, done);
      })
      .fail(function () {
        if (fallbackCtx && fallbackModel) {
          resolveInnAndPatchWithApi(self, fallbackCtx, fallbackModel, settings, done);
        } else {
          done({ inn: '', patchMeta: null });
        }
      });
  }

  function resolveInnForCard(self, ctx, settings, done) {
    if (!ctx || !ctx.id) {
      done({ inn: '', patchMeta: null });
      return;
    }
    if (ctx.kind === 'leads') {
      resolveInnForLeadCard(self, ctx.id, settings, done, ctx, ctx.model);
      return;
    }
    resolveInnAndPatchWithApi(self, ctx, ctx.model, settings, done);
  }

  /** Сделка: только доп. поля. Компания: имя + доп. поля. */
  function buildAmoPayload(kind, dadataRow, settings) {
    var cfv = [];
    /** Поля типа «число» в amo (как linked-form__field-numeric) — value должен быть number в JSON, не строка. */
    function amoCoercedValue(fieldKey, val) {
      var s = String(val == null ? '' : val).trim();
      if (!s.length) return null;
      var c = s.replace(/\s/g, '');
      var onlyDigits = /^\d+$/.test(c);
      if (fieldKey === 'field_rs' || fieldKey === 'field_corr') {
        if (onlyDigits) {
          var nAcc = parseInt(c, 10);
          if (Number.isSafeInteger(nAcc)) return nAcc;
        }
        return s;
      }
      if (
        fieldKey === 'field_inn' ||
        fieldKey === 'field_kpp' ||
        fieldKey === 'field_ogrn' ||
        fieldKey === 'field_bic' ||
        fieldKey === 'field_okpo'
      ) {
        if (onlyDigits) return parseInt(c, 10);
      }
      return s;
    }
    function add(fieldKey, val) {
      var raw = settings[fieldKey];
      if (!raw || !String(raw).trim()) return;
      var fid = parseInt(String(raw).trim(), 10);
      if (!fid) return;
      if (val === undefined || val === null) return;
      var v = amoCoercedValue(fieldKey, val);
      if (v === null || v === '') return;
      cfv.push({ field_id: fid, values: [{ value: v }] });
    }

    add('field_inn', dadataRow.inn);
    add('field_company_name', dadataRow.name);
    add('field_kpp', dadataRow.kpp);
    add('field_ogrn', dadataRow.ogrn);
    add('field_bic', dadataRow.bic);
    add('field_bank', dadataRow.bank_name);
    add('field_rs', dadataRow.settlement_account);
    add('field_corr', dadataRow.corr_account);
    add('field_address', dadataRow.address);
    add('field_director', dadataRow.director);
    add('field_status', dadataRow.status);
    add('field_okpo', dadataRow.okpo);
    add('field_okved', dadataRow.okved);
    add('field_registration_date', dadataRow.registration_date);
    add('field_opf', dadataRow.opf);

    if (kind === 'leads') {
      if (!cfv.length) return null;
      return { custom_fields_values: cfv };
    }

    var payload = {};
    if (dadataRow.name) payload.name = dadataRow.name;
    if (cfv.length) payload.custom_fields_values = cfv;
    if (!payload.name && !payload.custom_fields_values) return null;
    return payload;
  }

  var suggestState = { seq: 0, timer: null };

  function hideSuggestDropdown() {
    var $b = $('.js-inn-dadata-suggest');
    $b.empty().hide();
  }

  function renderSuggestDropdown(items) {
    var $box = $('.js-inn-dadata-suggest');
    if (!items || !items.length) {
      $box.empty().hide();
      return;
    }
    var esc = function (s) {
      return String(s == null ? '' : s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/"/g, '&quot;');
    };
    var html = items
      .map(function (it) {
        var inn = String(it.inn || '').replace(/\D/g, '');
        if (inn.length !== 10 && inn.length !== 12) return '';
        return (
          '<li class="inn-dadata-suggest__item js-inn-dadata-suggest-item" data-inn="' +
          esc(inn) +
          '" tabindex="-1"><span class="inn-dadata-suggest__label">' +
          esc(it.label || inn) +
          '</span><span class="inn-dadata-suggest__inn">' +
          esc(inn) +
          '</span></li>'
        );
      })
      .join('');
    if (!html) {
      $box.empty().hide();
      return;
    }
    $box.html(html).show();
  }

  function runSuggestRequest(self, settings, q) {
    if (!String(settings.backend_url || '').trim() || !String(settings.x_api_key || '').trim()) return;
    if (q.length < 2) {
      hideSuggestDropdown();
      return;
    }
    suggestState.seq += 1;
    var mySeq = suggestState.seq;
    var Ls = function (k, fb) {
      return tr(self, k, fb);
    };
    var $msg = $('.js-inn-dadata-msg');
    backendAmoProxyPost(self, settings, '/suggest-party', { query: q }, function (r) {
      if (mySeq !== suggestState.seq) return;
      if (!r.ok) {
        hideSuggestDropdown();
        var hint = '';
        if (r.status === 404) {
          hint = Ls(
            'err_suggest_404',
            'Сервер отвечает 404 на /suggest-party — задеплойте актуальный main на Render и обновите script.js в том месте, откуда подключается виджет.',
          );
        } else if (r.status === 403) {
          hint = Ls('err_suggest_403', 'Проверьте X-API-KEY в настройках виджета.');
        } else if (r.status === 503) {
          hint = Ls('err_suggest_503', 'На сервере не настроен DADATA_API_KEY.');
        } else if (r.status === 502 || r.err === 'parse') {
          hint = Ls('err_suggest_parse', 'Некорректный ответ сервера на /suggest-party.');
        } else if (r.status === 0 || r.err === 'network') {
          hint = Ls(
            'err_suggest_net',
            'Сеть: не удалось вызвать подсказки (crm_post и fetch). Проверьте URL, HTTPS и ключ.',
          );
        } else {
          hint = Ls('err_suggest', 'Подсказки недоступны') + ' (HTTP ' + r.status + ')';
        }
        $msg.text(hint);
        setTimeout(function () {
          $msg.text('');
        }, 12000);
        return;
      }
      $msg.text('');
      renderSuggestDropdown((r.body && r.body.suggestions) || []);
    });
  }

  /**
   * Тот же сценарий, что ручной curl: POST /integrations/amo/webhook + lead_id.
   * Сервер читает сделку в amo, берёт ИНН, DaData, PATCH — без amoApiV4 в браузере.
   */
  function requestServerLeadSync(self, settings, leadId, onDone) {
    var base = String(settings.backend_url || '').trim().replace(/\/+$/, '');
    var key = String(settings.x_api_key || '').trim();
    var lid = parseInt(String(leadId), 10);
    if (!base || !key || isNaN(lid) || lid <= 0) {
      if (onDone) onDone(false, null);
      return;
    }
    window.__innDadataBusy = true;
    var whBody = { lead_id: lid };
    var _fi = parseInt(String(settings.field_inn || '').trim(), 10);
    if (!isNaN(_fi) && _fi > 0) whBody.field_inn = _fi;
    var _fic = parseInt(String(settings.field_inn_company || '').trim(), 10);
    if (!isNaN(_fic) && _fic > 0) whBody.field_inn_company = _fic;
    backendAmoProxyPost(self, settings, '/integrations/amo/webhook', whBody, function (r) {
      try {
        var body = r.body;
        var success = !!(r.ok && body && body.ok === true);
        devTrace(self, settings, 'POST /integrations/amo/webhook', {
          leadId: lid,
          httpOk: !!r.ok,
          status: r.status,
          success: success,
          reason: body && body.reason,
          fields_updated: body && body.fields_updated,
          updated_entity: body && body.updated_entity,
          lead_mirror_fields_updated: body && body.lead_mirror_fields_updated,
          innLen: body && body.inn ? String(body.inn).replace(/\D/g, '').length : 0,
          transport:
            self && typeof self.crm_post === 'function'
              ? 'crm_post_then_fetch_fallback'
              : typeof fetch === 'function'
                ? 'fetch_only'
                : 'none',
          err: success ? undefined : r.err,
        });
        if (success && body.inn) {
          self._innDadataLastProcessedInn = String(body.inn).replace(/\D/g, '');
        } else if (body && body.reason === 'BAD_INN') {
          self._innDadataLastProcessedInn = '';
        }
        if (onDone) onDone(success, body);
      } finally {
        window.__innDadataBusy = false;
      }
    });
  }

  /** Сообщение «Заполнено» только если сервер реально записал поля в amo. */
  function showAutoMsgIfWebhookUpdated(self, body) {
    var st = self.get_settings() || {};
    devTrace(self, st, 'авто-синх (ответ сервера)', {
      ok: !!(body && body.ok),
      fields_updated: body && body.fields_updated,
      reason: (body && body.reason) || null,
      updated_entity: (body && body.updated_entity) || null,
      lead_mirror_fields_updated: body && body.lead_mirror_fields_updated,
    });
    if (!body || !body.ok || !(body.fields_updated > 0)) {
      if (isDeveloperMode(st)) {
        var $m = $('.js-inn-dadata-msg');
        var parts = ['[dev] авто'];
        if (body) {
          parts.push('ok=' + body.ok);
          parts.push('fields_updated=' + (body.fields_updated != null ? body.fields_updated : '—'));
          if (body.reason) parts.push('reason=' + body.reason);
          if (body.hint) parts.push('hint=' + String(body.hint).slice(0, 160));
        } else parts.push('нет JSON в ответе');
        $m.text(parts.join(' · '));
      }
      return;
    }
    var $m2 = $('.js-inn-dadata-msg');
    $m2.text(tr(self, 'auto_ok', 'Заполнено из DaData'));
    setTimeout(function () {
      $m2.text('');
    }, 4000);
  }

  /**
   * На сделке сначала вызывается webhook на Render; если из браузера он не сработал или
   * fields_updated=0 — дублируем сценарий в карточке: company-by-inn + PATCH через amoApiV4
   * (как при ручном заполнении). Так обходим случаи, когда curl к webhook работает, а запрос из iframe — нет.
   */
  function maybeRunClientAfterLeadWebhook(self, st, live, webhookOk, body, fromButton) {
    var fu = body && Number(body.fields_updated);
    var serverOk = !!(body && body.ok === true);
    var needClient = !webhookOk || !serverOk || !(fu > 0);
    if (!needClient) return;
    resolveInnForCard(self, live, st, function (resolved) {
      if (window.__innDadataBusy) return;
      var inn = resolved.inn;
      if (inn.length !== 10 && inn.length !== 12) {
        if (fromButton) {
          alert(
            tr(self, 'err_api', 'Ошибка') +
              ': webhook не обновил сделку, ИНН в карточке не найден — проверьте field_inn / field_inn_company.',
          );
        }
        return;
      }
      var skipDup =
        inn === self._innDadataLastProcessedInn &&
        webhookOk &&
        serverOk &&
        fu > 0;
      if (skipDup) return;
      devTrace(self, st, 'клиентский fallback после webhook', {
        webhookOk: webhookOk,
        bodyOk: serverOk,
        fields_updated: body && body.fields_updated,
        innLen: inn.length,
        fromButton: !!fromButton,
      });
      runPipeline(self, st, live, inn, !!fromButton, resolved.patchMeta);
    });
  }

  /**
   * Сделка при заданном backend_url: webhook на Render читает сделку через REST amo — там только
   * уже сохранённый ИНН. Если пользователь меняет ИНН в карточке и не нажал «Сохранить», в API
   * остаётся старое значение → «без изменений». Сначала берём ИНН из модели карточки (resolveInnAndPatch)
   * и runPipeline; иначе webhook + fallback как раньше.
   */
  function handleLeadCardHybridSync(self, live, st) {
    var hb =
      String(st.backend_url || '').trim().length > 0 &&
      String(st.x_api_key || '').trim().length > 0;
    if (!hb || live.kind !== 'leads') return;
    var quick = resolveInnAndPatch(live, live.model, st);
    function continueHybrid(q) {
      if ((q.inn.length === 10 || q.inn.length === 12) && q.inn !== self._innDadataLastProcessedInn) {
        devTrace(self, st, 'сделка: ИНН → runPipeline', {
          innLen: q.inn.length,
          hasPatchMeta: !!(q.patchMeta && q.patchMeta.kind),
        });
        runPipeline(self, st, live, q.inn, false, q.patchMeta);
        return;
      }
      if (q.inn.length !== 10 && q.inn.length !== 12) {
        if (q.inn.length > 0) {
          devTrace(self, st, 'ИНН из поля: длина не 10 и не 12 — runPipeline не вызываем, дальше webhook', {
            innLen: q.inn.length,
          });
        }
        self._innDadataLastProcessedInn = '';
      } else if (q.inn === self._innDadataLastProcessedInn) {
        return;
      }
      devTrace(self, st, 'событие модели → webhook', { leadId: live.id });
      requestServerLeadSync(self, st, live.id, function (ok, body) {
        if (ok) showAutoMsgIfWebhookUpdated(self, body);
        maybeRunClientAfterLeadWebhook(self, st, live, ok, body, false);
      });
    }
    if ((quick.inn.length === 10 || quick.inn.length === 12) && quick.inn !== self._innDadataLastProcessedInn) {
      continueHybrid(quick);
      return;
    }
    /* ИНН в блоке компании в UI часто не попадает в модель сделки до sync — подтягиваем lead?with=companies */
    resolveInnForLeadCard(
      self,
      live.id,
      st,
      function (resolved) {
        var r = resolved || { inn: '', patchMeta: null };
        if ((r.inn.length === 10 || r.inn.length === 12) && r.inn !== self._innDadataLastProcessedInn) {
          continueHybrid(r);
          return;
        }
        continueHybrid(quick);
      },
      live,
      live.model,
    );
  }

  function runPipeline(self, settings, ctx, inn, fromButton, patchMeta) {
    var L = function (k, fb) {
      return tr(self, k, fb);
    };
    if (!String(settings.backend_url || '').trim() || !String(settings.x_api_key || '').trim()) {
      if (fromButton) alert(L('err_settings', 'Заполните URL бэкенда и X-API-KEY.'));
      return;
    }
    if (inn.length !== 10 && inn.length !== 12) {
      if (fromButton) alert(L('err_inn', 'ИНН из 10 или 12 цифр.'));
      return;
    }

    var effKind = patchMeta && patchMeta.kind ? patchMeta.kind : ctx.kind;
    var effId = patchMeta && patchMeta.id != null ? patchMeta.id : ctx.id;

    devTrace(self, settings, 'runPipeline start', {
      innLen: inn.length,
      effKind: effKind,
      effId: String(effId),
      ctxKind: ctx.kind,
      fromButton: !!fromButton,
      hasPatchMeta: !!(patchMeta && patchMeta.kind),
      lastProcessedBefore: self._innDadataLastProcessedInn || '',
    });

    var $msg = $('.js-inn-dadata-msg');
    $msg.text('…');

    window.__innDadataBusy = true;
    backendAmoProxyPost(self, settings, '/company-by-inn', { inn: inn }, function (r) {
      function finishBusy() {
        window.__innDadataBusy = false;
      }
      function failMsg(text) {
        $msg.text('');
        devTrace(self, settings, 'runPipeline error', { message: text });
        if (fromButton) alert(L('err_api', 'Ошибка') + ': ' + text);
        finishBusy();
      }

      if (r.err === 'no_base_or_key') {
        failMsg(L('err_settings', 'Заполните URL бэкенда и X-API-KEY.'));
        return;
      }
      if (!r.ok) {
        if (r.status === 404 && r.body && r.body.error === 'NOT_FOUND') {
          failMsg('Не найдено в DaData');
          return;
        }
        var d = r.body && r.body.detail;
        var err = r.body && r.body.error;
        var msg =
          typeof d === 'string'
            ? d
            : typeof err === 'string'
              ? err
              : r.err === 'network'
                  ? 'Сеть: не удалось вызвать бэкенд'
                  : r.err === 'parse'
                    ? 'Некорректный ответ сервера'
                    : JSON.stringify(r.body || r.status);
        failMsg(msg);
        return;
      }
      var data = r.body;
      if (data && data.error) {
        failMsg(data.error);
        return;
      }
      try {
        var payload = buildAmoPayload(effKind, data, settings);
        var _cfv = payload && payload.custom_fields_values;
        devTrace(self, settings, 'runPipeline buildAmoPayload', {
          effKind: effKind,
          payloadNull: !payload,
          cfvCount: _cfv && _cfv.length,
          hasName: !!(payload && payload.name),
          dadataKeys: data && typeof data === 'object' ? Object.keys(data).slice(0, 12) : [],
          transport:
            self && typeof self.crm_post === 'function'
              ? 'crm_post_then_fetch_fallback'
              : typeof fetch === 'function'
                ? 'fetch_only'
                : 'none',
        });
        if (!payload) {
          throw new Error(
            'Нет полей для записи: укажите ID доп. полей сделки/компании в настройках виджета.',
          );
        }
        var apiPath = effKind === 'leads' ? '/api/v4/leads/' : '/api/v4/companies/';
        var deferred = $.Deferred();
        var xhrPatch = amoApiV4(self, {
          url: apiPath + effId,
          method: 'PATCH',
          contentType: 'application/json',
          data: JSON.stringify(payload),
        });
        if (!xhrPatch) {
          deferred.reject(new Error('Нет авторизованного AJAX amo (amoApiV4)'));
        } else {
          xhrPatch
            .done(function () {
              devTrace(self, settings, 'amo PATCH ok', { effKind: effKind, effId: effId });
              deferred.resolve();
            })
            .fail(function (xhr) {
              devTrace(self, settings, 'amo PATCH fail', {
                effKind: effKind,
                effId: effId,
                status: xhr && xhr.status,
                responseSlice: ((xhr && xhr.responseText) || '').slice(0, 240),
              });
              var t = (xhr && xhr.responseText) || '';
              deferred.reject(new Error(t || L('err_amo', 'Ошибка amo')));
            });
        }
        deferred
          .promise()
          .then(function () {
            self._innDadataLastProcessedInn = inn;
            $msg.text('');
            if (fromButton) {
              alert(L('ok', 'Готово. Обновите страницу (F5), если поля не обновились.'));
            } else {
              $msg.text(L('auto_ok', 'Заполнено из DaData'));
              setTimeout(function () {
                $msg.text('');
              }, 4000);
            }
          })
          .catch(function (e) {
            failMsg((e && e.message) || String(e));
          })
          .always(finishBusy);
      } catch (e) {
        failMsg((e && e.message) || String(e));
      }
    });
  }

  function detachInnWatcher(self) {
    if (self._innDadataModel && self._innDadataHandler) {
      try {
        self._innDadataModel.off('change:custom_fields_values', self._innDadataHandler);
        self._innDadataModel.off('change', self._innDadataHandler);
        self._innDadataModel.off('sync', self._innDadataHandler);
      } catch (e) {
        /* ignore */
      }
    }
    self._innDadataModel = null;
    self._innDadataHandler = null;
    self._innDadataLastProcessedInn = '';
    if (self._innDadataDebounce) {
      clearTimeout(self._innDadataDebounce);
      self._innDadataDebounce = null;
    }
    if (self._innPollTimer) {
      clearInterval(self._innPollTimer);
      self._innPollTimer = null;
    }
    if (self._innAttachRetryTimer) {
      clearTimeout(self._innAttachRetryTimer);
      self._innAttachRetryTimer = null;
    }
    if (self._innPollReattachTimer) {
      clearTimeout(self._innPollReattachTimer);
      self._innPollReattachTimer = null;
    }
    self._innWatcherCardKey = '';
  }

  function attachInnWatcher(self) {
    var settings = self.get_settings() || {};
    var innFid = parseInt(String(settings.field_inn || '').trim(), 10);
    var compInnFid = parseInt(String(settings.field_inn_company || '').trim(), 10);
    var innFieldAny = innFid || compInnFid;
    var hasBackend =
      String(settings.backend_url || '').trim().length > 0 &&
      String(settings.x_api_key || '').trim().length > 0;

    var ctx = currentCardContext(self);
    if (!ctx || !ctx.model) {
      self._innAttachAttempt = (self._innAttachAttempt || 0) + 1;
      if (self._innAttachAttempt === 24 && isDeveloperMode(settings)) {
        try {
          if (
            typeof APP !== 'undefined' &&
            APP.data &&
            APP.data.current_card &&
            (APP.data.current_card.id === 0 || APP.data.current_card.id === '0')
          ) {
            devTrace(
              self,
              settings,
              'id сделки = 0 (черновик) — сохраните карточку; пока виджет не шлёт запросы на бэкенд',
              {},
            );
          }
        } catch (eId0) {
          /* ignore */
        }
      }
      if (self._innAttachAttempt > 80) return;
      if (self._innAttachRetryTimer) clearTimeout(self._innAttachRetryTimer);
      self._innAttachRetryTimer = setTimeout(function () {
        self._innAttachRetryTimer = null;
        attachInnWatcher(self);
      }, 350);
      return;
    }

    var newKey = cardContextKey(ctx);
    if (
      self._innWatcherCardKey === newKey &&
      self._innDadataModel === ctx.model &&
      self._innDadataHandler
    ) {
      self._innAttachAttempt = 0;
      return;
    }

    if (ctx.kind === 'companies' && !innFieldAny) return;
    if (ctx.kind === 'leads' && !innFieldAny && !hasBackend) return;

    detachInnWatcher(self);

    self._innAttachAttempt = 0;
    self._innWatcherCardKey = newKey;
    self._innDadataModel = ctx.model;
    self._innDadataLastProcessedInn = '';

    self._innDadataHandler = function () {
      if (window.__innDadataBusy) return;
      var c = currentCardContext(self);
      if (!c || cardContextKey(c) !== self._innWatcherCardKey) return;
      clearTimeout(self._innDadataDebounce);
      self._innDadataDebounce = setTimeout(function () {
        var live = currentCardContext(self);
        if (!live || cardContextKey(live) !== self._innWatcherCardKey) return;
        var st = self.get_settings() || {};
        var hb =
          String(st.backend_url || '').trim().length > 0 &&
          String(st.x_api_key || '').trim().length > 0;
        if (live.kind === 'leads' && hb) {
          handleLeadCardHybridSync(self, live, st);
          return;
        }
        var fid = parseInt(String(st.field_inn || '').trim(), 10);
        if (!fid) return;
        resolveInnForCard(self, live, st, function (resolved) {
          var inn = resolved.inn;
          devTrace(self, st, 'клиентский pipeline: ИНН', {
            innLen: inn.length,
            lastProcessed: self._innDadataLastProcessedInn || '',
            skipDuplicate: inn === self._innDadataLastProcessedInn,
            hasPatchMeta: !!(resolved.patchMeta && resolved.patchMeta.kind),
          });
          if (inn.length !== 10 && inn.length !== 12) {
            self._innDadataLastProcessedInn = '';
            return;
          }
          if (inn === self._innDadataLastProcessedInn) return;
          runPipeline(self, st, live, inn, false, resolved.patchMeta);
        });
      }, 650);
    };

    ctx.model.on('change', self._innDadataHandler);
    ctx.model.on('change:custom_fields_values', self._innDadataHandler);
    ctx.model.on('sync', self._innDadataHandler);

    self._innPollTimer = setInterval(function () {
      if (window.__innDadataBusy) return;
      var st = self.get_settings() || {};
      var hb =
        String(st.backend_url || '').trim().length > 0 &&
        String(st.x_api_key || '').trim().length > 0;
      var cc = currentCardContext(self);
      if (!cc) return;
      if (cardContextKey(cc) !== self._innWatcherCardKey) {
        if (self._innPollReattachTimer) clearTimeout(self._innPollReattachTimer);
        self._innPollReattachTimer = setTimeout(function () {
          self._innPollReattachTimer = null;
          attachInnWatcher(self);
        }, 150);
        return;
      }
      if (!self._innDadataModel) return;
      if (cc.kind === 'leads' && hb) {
        handleLeadCardHybridSync(self, cc, st);
        return;
      }
      if (cc.kind !== 'leads') return;
      var fid = parseInt(String(st.field_inn || '').trim(), 10);
      if (!fid) return;
      resolveInnForCard(self, cc, st, function (r) {
        if (r.inn.length !== 10 && r.inn.length !== 12) return;
        if (r.inn === self._innDadataLastProcessedInn) return;
        runPipeline(self, st, cc, r.inn, false, r.patchMeta);
      });
    }, 6000);
  }

  return function () {
    var self = this;

    this.callbacks = {
      init: function () {
        return true;
      },

      bind_actions: function () {
        try {
          var st0 = self.get_settings() || {};
          var host0 = '';
          try {
            var bu = String(st0.backend_url || '').trim();
            if (bu) host0 = new URL(bu).host;
          } catch (eHost) {
            host0 = '(некорректный URL)';
          }
          devTrace(self, st0, 'виджет подключён: backend host, crm_post', {
            devtoolsSearch: WIDGET_F12_SEARCH,
            backendHost: host0 || '(пусто)',
            hasKey: String(st0.x_api_key || '').trim().length > 0,
            crm_post: typeof self.crm_post === 'function',
          });
        } catch (eBind) {
          /* ignore */
        }
        $(document)
          .off('input.innDadataSuggest')
          .on('input.innDadataSuggest', '.js-inn-dadata-input', function () {
            var q = String($(this).val() || '').trim();
            clearTimeout(suggestState.timer);
            suggestState.timer = setTimeout(function () {
              var st = self.get_settings() || {};
              runSuggestRequest(self, st, q);
            }, 260);
          });
        $(document)
          .off('mousedown.innDadataSuggestPick')
          .on('mousedown.innDadataSuggestPick', '.js-inn-dadata-suggest-item', function (e) {
            e.preventDefault();
            var inn = String($(this).data('inn') || '').replace(/\D/g, '');
            if (inn.length !== 10 && inn.length !== 12) return;
            hideSuggestDropdown();
            $('.js-inn-dadata-input').val('');
            var ctx = currentCardContext(self);
            if (!ctx) return;
            var st = self.get_settings() || {};
            resolveInnForCard(self, ctx, st, function (res) {
              var pm = res.inn === inn ? res.patchMeta : null;
              runPipeline(self, st, ctx, inn, false, pm);
            });
          });
        $(document)
          .off('click.innDadataSuggestClose')
          .on('click.innDadataSuggestClose', function (e) {
            if ($(e.target).closest('.inn-dadata-widget__suggest-wrap').length) return;
            hideSuggestDropdown();
          });
        $(document)
          .off('click.innDadata')
          .on('click.innDadata', '.js-inn-dadata-fill', function () {
            var settings = self.get_settings() || {};
            var L = function (k, fb) {
              return tr(self, k, fb);
            };
            var ctx = currentCardContext(self);
            if (!ctx) {
              alert(L('err_card', 'Откройте сохранённую карточку сделки или компании.'));
              return;
            }
            var fromInput = ($('.js-inn-dadata-input').val() || '').replace(/\D/g, '');
            if (fromInput.length === 10 || fromInput.length === 12) {
              runPipeline(self, settings, ctx, fromInput, true, null);
              return;
            }
            if (ctx.kind === 'leads') {
              requestServerLeadSync(self, settings, ctx.id, function (ok, body) {
                if (ok && body && body.ok === true && Number(body.fields_updated) > 0) {
                  alert(L('ok', 'Готово. Обновите страницу (F5), если поля не обновились.'));
                  return;
                }
                maybeRunClientAfterLeadWebhook(self, settings, ctx, ok, body, true);
              });
              return;
            }
            resolveInnForCard(self, ctx, settings, function (resolved) {
              var inn = resolved.inn;
              if (inn.length !== 10 && inn.length !== 12) {
                alert(L('err_inn', 'ИНН из 10 или 12 цифр.'));
                return;
              }
              runPipeline(self, settings, ctx, inn, true, resolved.patchMeta);
            });
          });
        return true;
      },

      render: function () {
        var ctx = currentCardContext(self);
        if (!ctx) {
          return true;
        }

        var L = function (k, fb) {
          return tr(self, k, fb);
        };
        var pack = langPack(self);
        var title =
          (pack.widget && pack.widget.name) || tr(self, 'widget.name', 'ИНН → DaData');
        var v = self.get_version();
        var path = self.params.path;

        var hint =
          ctx.kind === 'leads'
            ? L(
                'hint_lead',
                'Реквизиты подтягивает ваш сервер по ИНН сделки/компании (как POST /integrations/amo/webhook). Кнопка «Заполнить» без ввода — тот же запрос. Поле ИНН в настройках для сделки не обязательно, если заданы URL и X-API-KEY.',
              )
            : L('hint_company', 'Укажите ИНН в поле компании или ниже, нажмите кнопку.');

        var stRender = self.get_settings() || {};
        var devBlock = '';
        if (isDeveloperMode(stRender)) {
          devBlock =
            '<p class="inn-dadata-widget__dev-hint">' +
            L(
              'dev_panel_hint',
              'Режим разработчика: консоль (F12) и лог ниже. См. УСТАНОВКА.txt — раздел про авто по ИНН.',
            ) +
            '</p><pre class="inn-dadata-widget__dev js-inn-dadata-dev" aria-live="polite"></pre>';
        }

        var css =
          '<link type="text/css" rel="stylesheet" href="' +
          path +
          '/style.css?v=' +
          v +
          '">';
        var inner =
          '<div class="inn-dadata-widget">' +
          '<p class="inn-dadata-widget__hint">' +
          hint +
          '</p>' +
          '<div class="inn-dadata-widget__row inn-dadata-widget__suggest-wrap">' +
          '<input type="text" class="inn-dadata-widget__input js-inn-dadata-input" placeholder="' +
          L('inn_placeholder', 'ИНН или название — подсказки при вводе') +
          '" maxlength="100" autocomplete="off"/>' +
          '<ul class="inn-dadata-suggest js-inn-dadata-suggest" role="listbox" aria-hidden="true"></ul>' +
          '</div>' +
          '<button type="button" class="inn-dadata-widget__btn js-inn-dadata-fill">' +
          L('button', 'Заполнить из DaData') +
          '</button>' +
          '<div class="inn-dadata-widget__msg js-inn-dadata-msg"></div>' +
          devBlock +
          '</div>';

        self.render_template({
          caption: {
            class_name: 'inn-dadata-widget-caption',
            html:
              '<img src="' +
              path +
              '/images/logo_min.png?v=' +
              v +
              '" alt="" style="max-height:18px;vertical-align:middle;margin-right:6px;"/>' +
              title,
          },
          body: css,
          render: inner,
        });

        setTimeout(function () {
          self._innAttachAttempt = 0;
          attachInnWatcher(self);
          var st0 = self.get_settings() || {};
          var cx0 = currentCardContext(self);
          if (
            cx0 &&
            cx0.kind === 'leads' &&
            String(st0.backend_url || '').trim() &&
            String(st0.x_api_key || '').trim() &&
            cx0.id
          ) {
            setTimeout(function () {
              var st1 = self.get_settings() || {};
              var cx1 = currentCardContext(self);
              if (cx1 && cx1.kind === 'leads' && cx1.id) {
                handleLeadCardHybridSync(self, cx1, st1);
              }
            }, 2000);
          }
        }, 600);

        return true;
      },

      settings: function () {
        return true;
      },

      destroy: function () {
        $(document).off('click.innDadata');
        $(document).off('input.innDadataSuggest');
        $(document).off('mousedown.innDadataSuggestPick');
        $(document).off('click.innDadataSuggestClose');
        hideSuggestDropdown();
        detachInnWatcher(self);
        return true;
      },
    };

    return this;
  };
});
