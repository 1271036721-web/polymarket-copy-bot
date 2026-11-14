#!/usr/bin/env python3
"""健康检查小服务（独立于机器人，不改机器人代码）。

用途：
1. 给 Cloady 一个可监听的端口：平台要求应用有个服务端口，
   机器人本身只出站连 Polymarket、不监听任何端口。
2. 给外部监控一个可 ping 的地址：免费档 7 天无流量会暂停应用，
   机器人自身的流量（RTDS/轮询）按官方定义已超过 10 MB/7 天，
   但多一个可被 uptime 监控 ping 的端点更稳妥。
3. 顺手暴露机器人状态：读 /data/state.json（只读），看现金/持仓/跟单数。

只用 Python 标准库，无额外依赖。监听 0.0.0.0:$PORT（默认 3000）。
"""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

STATE_FILE = Path(os.environ.get("BOT_STATE_FILE", "/data/state.json"))
PORT = int(os.environ.get("PORT", "3000"))


def read_state():
    try:
        s = json.loads(STATE_FILE.read_text())
        return {
            "cash": round(float(s.get("cash", 0)), 2),
            "positions": len(s.get("positions", {})),
            "trades": s.get("trades", 0),
            "pending": len(s.get("pending", {})),
        }
    except Exception as e:  # state 不存在/损坏都不应让健康检查挂掉
        return {"error": f"state unreadable: {e}"}


class Handler(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/health"):
            self._json(200, {
                "status": "ok",
                "service": "polymarket-copy-bot-v2",
                "state": read_state(),
            })
        else:
            self._json(404, {"status": "not_found"})

    def log_message(self, *args):  # 保持日志干净，不刷访问日志
        pass


if __name__ == "__main__":
    print(f"healthcheck listening on 0.0.0.0:{PORT} (state: {STATE_FILE})",
          flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
