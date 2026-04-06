#!/usr/bin/env python3
"""
Локальный мост: amo → ваш ПК (публичный URL через ngrok / cloudflared) → лог → пересылка на Render.

Зачем: увидеть в терминале, приходит ли запрос от amo, и одновременно прокинуть его на бэкенд.

1) Запуск моста:
     python scripts/amo_render_bridge.py
   По умолчанию слушает 0.0.0.0:8765

2) Публичный URL (выберите один вариант):
   - ngrok:  ngrok http 8765
   - cloudflared:  cloudflared tunnel --url http://127.0.0.1:8765

3) В роботе amo укажите (подставьте хост от ngrok):
     https://ВАШ-ПОДДОМЕН.ngrok-free.app/integrations/amo/ping
   или полный путь к /integrations/amo/webhook — путь после хоста мост пробрасывает как есть.

Переменные окружения:
  BRIDGE_TARGET  — куда пересылать (по умолчанию https://inn-efz1.onrender.com)
  BRIDGE_PORT    — порт (по умолчанию 8765)

Зависимости: только стандартная библиотека Python 3.10+.
"""
from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

TARGET = os.environ.get("BRIDGE_TARGET", "https://inn-efz1.onrender.com").strip().rstrip("/")
PORT = int(os.environ.get("BRIDGE_PORT", "8765"))


class BridgeHandler(BaseHTTPRequestHandler):
    server_version = "amo-render-bridge/1.0"

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("[bridge] " + fmt % args + "\n")

    def _read_body(self) -> bytes:
        raw = self.headers.get("Content-Length")
        if not raw:
            return b""
        try:
            n = int(raw)
        except ValueError:
            return b""
        return self.rfile.read(n) if n > 0 else b""

    def _dump_incoming(self, method: str, body: bytes) -> None:
        sys.stderr.write(f"\n=== amo → bridge {method} {self.path} ===\n")
        for k, v in self.headers.items():
            sys.stderr.write(f"  {k}: {v}\n")
        if body:
            preview = body[:4000]
            try:
                sys.stderr.write(preview.decode("utf-8", errors="replace") + "\n")
            except Exception:
                sys.stderr.write(f"<binary {len(body)} bytes>\n")
        sys.stderr.write("=== forward → " + TARGET + self.path.split("?")[0] + " ===\n\n")

    def _send_all(self, code: int, data: bytes, src_headers) -> None:
        self.send_response(code)
        ct = src_headers.get_content_type()
        if ct:
            self.send_header("Content-Type", ct)
        elif data and data[:1] in (b"{", b"["):
            self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if data and self.command != "HEAD":
            self.wfile.write(data)

    def _forward(self, method: str, body: bytes | None) -> None:
        path_qs = self.path if self.path.startswith("/") else "/" + self.path
        dest = TARGET + path_qs

        self._dump_incoming(method, body or b"")

        req = Request(dest, data=body if body else None, method=method)
        ct_in = self.headers.get("Content-Type")
        if body and ct_in:
            req.add_header("Content-Type", ct_in)
        elif body and method == "POST":
            req.add_header("Content-Type", "application/json")

        try:
            with urlopen(req, timeout=120) as resp:
                out = resp.read() if self.command != "HEAD" else b""
                self._send_all(resp.status, out, resp)
        except HTTPError as e:
            err_body = e.read()
            sys.stderr.write(f"[bridge] Render/цель ответила HTTP {e.code}\n")
            self._send_all(e.code, err_body, e)
        except URLError as e:
            sys.stderr.write(f"[bridge] Ошибка пересылки: {e}\n")
            msg = json.dumps({"ok": False, "bridge_error": str(e.reason or e)}).encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    def do_GET(self) -> None:
        self._forward("GET", None)

    def do_HEAD(self) -> None:
        self._forward("HEAD", None)

    def do_POST(self) -> None:
        self._forward("POST", self._read_body())


def main() -> None:
    host = "0.0.0.0"
    server = HTTPServer((host, PORT), BridgeHandler)
    sys.stderr.write(
        f"Мост слушает http://127.0.0.1:{PORT} → пересылка на {TARGET}\n"
        f"Запустите ngrok http {PORT} и вставьте в amo URL вида https://....ngrok-free.app/integrations/amo/ping\n"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.stderr.write("\nОстанов.\n")


if __name__ == "__main__":
    main()
