#!/usr/bin/env python3
"""RTDS 心跳测试：连接半开死掉（只超时、无报错、无数据）时，
rtds_loop 必须在 RTDS_SILENCE_LIMIT 秒内主动断开重连，而不是永远等下去。

背景：2026-11-16 21:00 / 22:04 / 23:12 / 23:18 四次 RTDS 断流假活——
进程活着、socket ESTAB、recv() 只超时、send("PING") 不报错，旧循环
永远发现不了，只能靠 watchdog 5 分钟后重启。心跳（Dylan 2026-11-16
选“两个都做”）把盲区压到 60 秒。

跑法：cd ~/workspace/polymarket-copy-bot && ./.venv/bin/python test_rtds_heartbeat.py
"""
import queue
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import copy_bot_v2


class FakeTimeout(Exception):
    pass


class FakeWS:
    """模拟一条半开死的连接：send 成功、recv 第一次给一条消息、之后永远超时。"""

    def __init__(self):
        self.sent = []
        self.recved = 0
        self.closed = False

    def send(self, msg):
        self.sent.append(msg)

    def settimeout(self, t):
        pass

    def recv(self):
        self.recved += 1
        if self.recved == 1:
            # 一条与目标无关的消息：快筛直接跳过，只证明“收到过数据”
            return '{"payload": {"proxyWallet": "0x0000000000000000000000000000000000000000"}}'
        raise FakeTimeout("timed out")

    def close(self):
        self.closed = True


class FakeWebsocketModule:
    WebSocketTimeoutException = FakeTimeout

    def __init__(self):
        self.connections = []

    def create_connection(self, url, timeout=None):
        ws = FakeWS()
        self.connections.append(ws)
        return ws


def make_stub():
    stub = type("Stub", (), {})()
    stub.stop = threading.Event()
    stub.backfill_needed = threading.Event()
    stub.events = queue.Queue()
    stub.stats = {
        "rtds_msgs": 0, "rtds_target_hits": 0, "rtds_latency": [],
        "rtds_reconnects": 0, "clob_reconnects": 0, "clob_msgs": 0,
        "poll_cycles": 0, "poll_errors": 0, "price_cache_hits": 0,
        "price_http_fallbacks": 0,
    }
    return stub


def run_loop_with_fake(stub, silence_limit, run_seconds):
    """用 fake websocket 跑 rtds_loop run_seconds 秒，返回 (fake 模块, 捕获的日志)。"""
    fake = FakeWebsocketModule()
    logs = []
    orig_ws = copy_bot_v2.websocket
    orig_log = copy_bot_v2.log
    orig_limit = copy_bot_v2.RTDS_SILENCE_LIMIT
    copy_bot_v2.websocket = fake
    copy_bot_v2.log = logs.append
    copy_bot_v2.RTDS_SILENCE_LIMIT = silence_limit
    t = threading.Thread(target=copy_bot_v2.Bot.rtds_loop, args=(stub,), daemon=True)
    try:
        t.start()
        time.sleep(run_seconds)
    finally:
        stub.stop.set()
        t.join(timeout=10)
        copy_bot_v2.websocket = orig_ws
        copy_bot_v2.log = orig_log
        copy_bot_v2.RTDS_SILENCE_LIMIT = orig_limit
    return fake, logs, t


def test_heartbeat_reconnects_on_silence():
    """半开死掉后，心跳必须在阈值内主动断开并重连（阈值压到 3 秒跑快测）。"""
    stub = make_stub()
    fake, logs, t = run_loop_with_fake(stub, silence_limit=3, run_seconds=10)
    assert not t.is_alive(), "rtds_loop 在 stop 后应退出"
    assert len(fake.connections) >= 2, (
        f"10 秒内应至少重连 1 次（实际新建连接 {len(fake.connections)} 次）")
    assert any("无数据，主动断开重连" in m for m in logs), (
        f"应记录心跳重连日志，实际日志：{logs[-6:]}")
    assert any("PING" in s for ws in fake.connections for s in ws.sent), "循环应正常发 PING"
    print(f"PASS 断流心跳重连：10 秒内新建连接 {len(fake.connections)} 次，"
          f"心跳日志已记录，PING 正常")


def test_no_heartbeat_when_healthy():
    """源源不断收到消息时，心跳不应触发重连。"""
    class HealthyWS(FakeWS):
        def recv(self):
            time.sleep(0.05)
            self.recved += 1
            return "PONG"

    class HealthyFake(FakeWebsocketModule):
        def create_connection(self, url, timeout=None):
            ws = HealthyWS()
            self.connections.append(ws)
            return ws

    fake = HealthyFake()
    logs = []
    stub = make_stub()
    orig_ws = copy_bot_v2.websocket
    orig_log = copy_bot_v2.log
    orig_limit = copy_bot_v2.RTDS_SILENCE_LIMIT
    copy_bot_v2.websocket = fake
    copy_bot_v2.log = logs.append
    copy_bot_v2.RTDS_SILENCE_LIMIT = 3
    t = threading.Thread(target=copy_bot_v2.Bot.rtds_loop, args=(stub,), daemon=True)
    try:
        t.start()
        time.sleep(8)
    finally:
        stub.stop.set()
        t.join(timeout=10)
        copy_bot_v2.websocket = orig_ws
        copy_bot_v2.log = orig_log
        copy_bot_v2.RTDS_SILENCE_LIMIT = orig_limit
    assert not t.is_alive(), "rtds_loop 在 stop 后应退出"
    assert len(fake.connections) == 1, (
        f"健康时 8 秒内不应重连（实际新建连接 {len(fake.connections)} 次）")
    assert not any("无数据，主动断开重连" in m for m in logs), "健康时不应记录心跳重连"
    print("PASS 健康不断流：8 秒内无重连、无心跳日志")


if __name__ == "__main__":
    test_heartbeat_reconnects_on_silence()
    test_no_heartbeat_when_healthy()
    print("ALL PASS (2/2)")
