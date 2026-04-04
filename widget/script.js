define(['jquery'], function ($) {
  'use strict';

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

  /** Карточка сделки или компании + id + backbone-модель */
  function currentCardContext() {
    if (typeof APP === 'undefined' || !APP.data || !APP.data.current_card) return null;
    var id = APP.data.current_card.id;
    if (id === 0 || id === '0') return null;
    var area = APP.getWidgetsArea && APP.getWidgetsArea();
    var model = APP.data.current_card.model;
    if (area === 'leads_card') return { kind: 'leads', id: id, model: model };
    if (area === 'companies_card') return { kind: 'companies', id: id, model: model };
    if (APP.isCard && APP.getBaseEntity && APP.getBaseEntity() === 'companies')
      return { kind: 'companies', id: id, model: model };
    if (APP.isCard && APP.getBaseEntity && APP.getBaseEntity() === 'leads')
      return { kind: 'leads', id: id, model: model };
    return null;
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
    var compFid = parseInt(String(settings.field_inn_company || '').trim(), 10) || leadFid;
    if (!leadFid || !model) return { inn: '', patchMeta: null };

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

  /** Сделка: только доп. поля. Компания: имя + доп. поля. */
  function buildAmoPayload(kind, dadataRow, settings) {
    var cfv = [];
    function add(fieldKey, val) {
      var raw = settings[fieldKey];
      if (!raw || !String(raw).trim()) return;
      var fid = parseInt(String(raw).trim(), 10);
      if (!fid) return;
      if (val === undefined || val === null) return;
      var s = String(val).trim();
      if (!s.length) return;
      cfv.push({ field_id: fid, values: [{ value: s }] });
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
    var base = String(settings.backend_url).trim().replace(/\/+$/, '');
    var Ls = function (k, fb) {
      return tr(self, k, fb);
    };
    var $msg = $('.js-inn-dadata-msg');
    fetch(base + '/suggest-party', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-KEY': String(settings.x_api_key).trim(),
      },
      body: JSON.stringify({ query: q }),
    })
      .then(function (res) {
        return res
          .json()
          .catch(function () {
            return {};
          })
          .then(function (body) {
            return { ok: res.ok, status: res.status, body: body };
          });
      })
      .then(function (r) {
        if (mySeq !== suggestState.seq) return;
        if (!r.ok) {
          hideSuggestDropdown();
          var hint = '';
          if (r.status === 404) {
            hint = Ls(
              'err_suggest_404',
              'Сервер отвечает 404 на /suggest-party — задеплойте актуальный main на Render и заново залейте script.js виджета в amo.',
            );
          } else if (r.status === 403) {
            hint = Ls('err_suggest_403', 'Проверьте X-API-KEY в настройках виджета.');
          } else if (r.status === 503) {
            hint = Ls('err_suggest_503', 'На сервере не настроен DADATA_API_KEY.');
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
        renderSuggestDropdown(r.body.suggestions || []);
      })
      .catch(function () {
        if (mySeq !== suggestState.seq) return;
        hideSuggestDropdown();
        $msg.text(
          Ls('err_suggest_net', 'Сеть: не удалось вызвать подсказки. Проверьте URL бэкенда и CORS/HTTPS.'),
        );
        setTimeout(function () {
          $msg.text('');
        }, 8000);
      });
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

    var base = String(settings.backend_url).trim().replace(/\/+$/, '');
    var url = base + '/company-by-inn';
    var $msg = $('.js-inn-dadata-msg');
    $msg.text('…');

    window.__innDadataBusy = true;
    fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-KEY': String(settings.x_api_key).trim(),
      },
      body: JSON.stringify({ inn: inn }),
    })
      .then(function (res) {
        return res.json().then(function (body) {
          return { ok: res.ok, status: res.status, body: body };
        });
      })
      .then(function (r) {
        if (r.status === 404 && r.body && r.body.error === 'NOT_FOUND') {
          throw new Error('Не найдено в DaData');
        }
        if (!r.ok) {
          var d = r.body && r.body.detail;
          var err = r.body && r.body.error;
          var msg =
            typeof d === 'string'
              ? d
              : typeof err === 'string'
                ? err
                : JSON.stringify(r.body || r.status);
          throw new Error(msg);
        }
        if (r.body && r.body.error) throw new Error(r.body.error);
        return r.body;
      })
      .then(function (data) {
        var payload = buildAmoPayload(effKind, data, settings);
        if (!payload) {
          throw new Error(
            'Нет полей для записи: укажите ID доп. полей сделки/компании в настройках виджета.',
          );
        }
        var apiPath = effKind === 'leads' ? '/api/v4/leads/' : '/api/v4/companies/';
        var deferred = $.Deferred();
        if (typeof self.$authorizedAjax !== 'function') {
          deferred.reject(new Error('Нет $authorizedAjax'));
          return deferred.promise();
        }
        self
          .$authorizedAjax({
            url: apiPath + effId,
            method: 'PATCH',
            contentType: 'application/json',
            data: JSON.stringify(payload),
          })
          .done(function () {
            deferred.resolve();
          })
          .fail(function (xhr) {
            var t = (xhr && xhr.responseText) || '';
            deferred.reject(new Error(t || L('err_amo', 'Ошибка amo')));
          });
        return deferred.promise();
      })
      .then(function () {
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
        $msg.text('');
        var text = (e && e.message) || String(e);
        if (fromButton) alert(L('err_api', 'Ошибка') + ': ' + text);
      })
      .always(function () {
        window.__innDadataBusy = false;
      });
  }

  function detachInnWatcher(self) {
    if (self._innDadataModel && self._innDadataHandler) {
      try {
        self._innDadataModel.off('change:custom_fields_values', self._innDadataHandler);
        self._innDadataModel.off('change', self._innDadataHandler);
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
  }

  function attachInnWatcher(self) {
    var settings = self.get_settings() || {};
    var innFid = parseInt(String(settings.field_inn || '').trim(), 10);
    if (!innFid) return;

    detachInnWatcher(self);

    var ctx = currentCardContext();
    if (!ctx || !ctx.model) return;

    self._innDadataModel = ctx.model;
    self._innDadataLastProcessedInn = '';
    self._innDadataHandler = function () {
      if (window.__innDadataBusy) return;
      var c = currentCardContext();
      if (!c || c.id !== ctx.id) return;
      clearTimeout(self._innDadataDebounce);
      self._innDadataDebounce = setTimeout(function () {
        var resolved = resolveInnAndPatch(c, self._innDadataModel, settings);
        var inn = resolved.inn;
        if (inn.length !== 10 && inn.length !== 12) {
          self._innDadataLastProcessedInn = '';
          return;
        }
        if (inn === self._innDadataLastProcessedInn) return;
        self._innDadataLastProcessedInn = inn;
        runPipeline(self, settings, c, inn, false, resolved.patchMeta);
      }, 550);
    };

    ctx.model.on('change', self._innDadataHandler);

    if (ctx.kind === 'leads') {
      self._innPollTimer = setInterval(function () {
        if (window.__innDadataBusy) return;
        var cc = currentCardContext();
        if (!cc || cc.id !== ctx.id || !self._innDadataModel) return;
        var r = resolveInnAndPatch(cc, self._innDadataModel, settings);
        if (r.inn.length !== 10 && r.inn.length !== 12) return;
        if (r.inn === self._innDadataLastProcessedInn) return;
        self._innDadataLastProcessedInn = r.inn;
        runPipeline(self, settings, cc, r.inn, false, r.patchMeta);
      }, 2500);
    }
  }

  return function () {
    var self = this;

    this.callbacks = {
      init: function () {
        return true;
      },

      bind_actions: function () {
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
            var ctx = currentCardContext();
            if (!ctx) return;
            var st = self.get_settings() || {};
            var res = resolveInnAndPatch(ctx, ctx.model, st);
            var pm = res.inn === inn ? res.patchMeta : null;
            runPipeline(self, st, ctx, inn, false, pm);
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
            var ctx = currentCardContext();
            if (!ctx) {
              alert(L('err_card', 'Откройте сохранённую карточку сделки или компании.'));
              return;
            }
            var fromInput = ($('.js-inn-dadata-input').val() || '').replace(/\D/g, '');
            var resolved = resolveInnAndPatch(ctx, ctx.model, settings);
            var inn = resolved.inn;
            var pm = resolved.patchMeta;
            if (fromInput.length === 10 || fromInput.length === 12) {
              inn = fromInput;
              pm = null;
            }
            if (inn.length !== 10 && inn.length !== 12) {
              alert(L('err_inn', 'ИНН из 10 или 12 цифр.'));
              return;
            }
            runPipeline(self, settings, ctx, inn, true, pm);
          });
        return true;
      },

      render: function () {
        var ctx = currentCardContext();
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
                'Введите ИНН в поле сделки (ID задан в настройках). Остальные поля подставятся автоматически.',
              )
            : L('hint_company', 'Укажите ИНН в поле компании или ниже, нажмите кнопку.');

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
          attachInnWatcher(self);
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
