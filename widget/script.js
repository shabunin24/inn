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

  function extractInnFromModel(model, fieldId) {
    if (!model || !fieldId) return '';
    var cfv = model.get('custom_fields_values');
    if (!cfv || !cfv.length) return '';
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

  function runPipeline(self, settings, ctx, inn, fromButton) {
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
        var payload = buildAmoPayload(ctx.kind, data, settings);
        if (!payload) {
          throw new Error(
            'Нет полей для записи: укажите ID доп. полей сделки/компании в настройках виджета.',
          );
        }
        var apiPath = ctx.kind === 'leads' ? '/api/v4/leads/' : '/api/v4/companies/';
        var deferred = $.Deferred();
        if (typeof self.$authorizedAjax !== 'function') {
          deferred.reject(new Error('Нет $authorizedAjax'));
          return deferred.promise();
        }
        self
          .$authorizedAjax({
            url: apiPath + ctx.id,
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
      } catch (e) {
        /* ignore */
      }
    }
    self._innDadataModel = null;
    self._innDadataHandler = null;
    if (self._innDadataDebounce) {
      clearTimeout(self._innDadataDebounce);
      self._innDadataDebounce = null;
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
    self._innDadataHandler = function () {
      if (window.__innDadataBusy) return;
      var c = currentCardContext();
      if (!c || c.id !== ctx.id) return;
      clearTimeout(self._innDadataDebounce);
      self._innDadataDebounce = setTimeout(function () {
        var inn = extractInnFromModel(self._innDadataModel, innFid);
        if (inn.length !== 10 && inn.length !== 12) return;
        runPipeline(self, settings, c, inn, false);
      }, 800);
    };

    ctx.model.on('change:custom_fields_values', self._innDadataHandler);
  }

  return function () {
    var self = this;

    this.callbacks = {
      init: function () {
        return true;
      },

      bind_actions: function () {
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
            var innFid = parseInt(String(settings.field_inn || '').trim(), 10);
            var inn = '';
            if (ctx.model && innFid) {
              inn = extractInnFromModel(ctx.model, innFid);
            }
            if (!inn) {
              inn = ($('.js-inn-dadata-input').val() || '').replace(/\D/g, '');
            }
            runPipeline(self, settings, ctx, inn, true);
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
          '<div class="inn-dadata-widget__row">' +
          '<input type="text" class="inn-dadata-widget__input js-inn-dadata-input" placeholder="' +
          L('inn_placeholder', 'ИНН (если не из поля сделки)') +
          '" maxlength="12"/>' +
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
        detachInnWatcher(self);
        return true;
      },
    };

    return this;
  };
});
