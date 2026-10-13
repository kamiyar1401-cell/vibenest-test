#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lightweight WS edge service for PaaS (Railway / VibeNest style).
Looks like a normal static site + health endpoint.
UUID must match client. Path is randomized-looking.
"""

import os
import sys
import uuid
import struct
import socket
import asyncio
import base64
import hashlib
import logging
from urllib.parse import urlparse

# ---- config (override via env) ----
UUID = os.environ.get("UUID", "5efabea4-f6d4-91fd-b8f0-17e004c89c60")
PORT = int(os.environ.get("PORT", os.environ.get("RAILWAY_PORT", "8080")))
WS_PATH = os.environ.get("WS_PATH", "/api/v1/stream")          # looks like API
DOMAIN = os.environ.get("DOMAIN", "localhost")                 # public hostname without https
NAME = os.environ.get("NAME", "edge")
DEBUG = os.environ.get("DEBUG", "0") == "1"

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("edge")

# ---- helpers ----
def uuid_bytes(u: str) -> bytes:
    return uuid.UUID(u).bytes

TARGET_UUID = uuid_bytes(UUID)

async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

# ---- VLESS header parse (minimal) ----
async def handle_vless(ws):
    """
    First binary frame from client = VLESS request header.
    We parse only what is needed then bidirectional pipe.
    """
    try:
        # receive first message
        msg = await ws.receive()
        if msg.type.name != "BINARY":
            await ws.close()
            return
        data = msg.data
        if len(data) < 18:
            await ws.close()
            return

        # version (1) + uuid (16) + addon_len (1)
        ver = data[0]
        if ver != 0:
            await ws.close()
            return
        uid = data[1:17]
        if uid != TARGET_UUID:
            await ws.close()
            return
        addon_len = data[17]
        pos = 18 + addon_len
        if pos + 3 > len(data):
            await ws.close()
            return

        cmd = data[pos]          # 1 = TCP
        port = struct.unpack("!H", data[pos + 1:pos + 3])[0]
        pos += 3
        atype = data[pos]
        pos += 1

        if atype == 1:           # IPv4
            host = socket.inet_ntoa(data[pos:pos + 4])
            pos += 4
        elif atype == 2:         # domain
            dlen = data[pos]
            host = data[pos + 1:pos + 1 + dlen].decode("utf-8", errors="ignore")
            pos += 1 + dlen
        elif atype == 3:         # IPv6
            host = socket.inet_ntop(socket.AF_INET6, data[pos:pos + 16])
            pos += 16
        else:
            await ws.close()
            return

        # remaining bytes after header = first payload
        first_payload = data[pos:]

        # reply with VLESS response header (version + 0 addon)
        await ws.send_bytes(b"\x00\x00")

        # connect upstream
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except Exception:
            await ws.close()
            return

        if first_payload:
            writer.write(first_payload)
            await writer.drain()

        # bidirectional relay
        async def ws_to_tcp():
            try:
                while True:
                    m = await ws.receive()
                    if m.type.name == "BINARY":
                        writer.write(m.data)
                        await writer.drain()
                    elif m.type.name in ("CLOSE", "ERROR"):
                        break
            except Exception:
                pass
            finally:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

        async def tcp_to_ws():
            try:
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        break
                    await ws.send_bytes(chunk)
            except Exception:
                pass
            finally:
                try:
                    await ws.close()
                except Exception:
                    pass

        await asyncio.gather(ws_to_tcp(), tcp_to_ws())
    except Exception as e:
        if DEBUG:
            log.exception("vless err: %s", e)
        try:
            await ws.close()
        except Exception:
            pass

# ---- HTTP facade (looks normal) ----
HTML_FAKE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Status</title>
<style>
body{font-family:system-ui,sans-serif;background:#0f1115;color:#e6e6e6;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.card{background:#1a1d24;border-radius:12px;padding:2rem 2.5rem;box-shadow:0 8px 32px rgba(0,0,0,.4);text-align:center}
h1{font-size:1.25rem;margin:0 0 .5rem;font-weight:600}
p{margin:0;opacity:.7;font-size:.9rem}
.dot{display:inline-block;width:8px;height:8px;background:#3dd68c;border-radius:50%;margin-right:6px}
</style>
</head>
<body>
<div class="card">
<h1><span class="dot"></span>Service Online</h1>
<p>Edge node healthy · {name}</p>
</div>
</body>
</html>
""".replace("{name}", NAME)

async def http_handler(request):
    from aiohttp import web
    path = request.path
    if path in ("/", "/health", "/status", "/ping"):
        return web.Response(text=HTML_FAKE, content_type="text/html")
    if path == "/robots.txt":
        return web.Response(text="User-agent: *\nDisallow: /\n", content_type="text/plain")
    # subscription-ish endpoint (optional)
    if path == "/sub" or path.endswith("/sub"):
        link = (
            f"vless://{UUID}@{DOMAIN}:443"
            f"?encryption=none&security=tls&type=ws&host={DOMAIN}"
            f"&path={WS_PATH.replace('/', '%2F')}#{NAME}"
        )
        b64 = base64.b64encode(link.encode()).decode()
        return web.Response(text=b64, content_type="text/plain")
    return web.Response(status=404, text="Not Found")

async def ws_handler(request):
    from aiohttp import web
    ws = web.WebSocketResponse(autoclose=True, autoping=True, heartbeat=30)
    await ws.prepare(request)
    await handle_vless(ws)
    return ws

async def main():
    from aiohttp import web
    app = web.Application()
    app.router.add_get("/", http_handler)
    app.router.add_get("/health", http_handler)
    app.router.add_get("/status", http_handler)
    app.router.add_get("/ping", http_handler)
    app.router.add_get("/robots.txt", http_handler)
    app.router.add_get("/sub", http_handler)
    app.router.add_get(WS_PATH, ws_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.warning("listening on :%s  path=%s  uuid=%s", PORT, WS_PATH, UUID)
    # keep alive
    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
