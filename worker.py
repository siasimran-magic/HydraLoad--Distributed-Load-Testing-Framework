#!/usr/bin/env python3
"""
Worker — connects to coordinator, receives config, fires raw HTTP
requests via TCP sockets, streams metrics back.

Usage:
  export LOAD_TEST_TOKEN=<same token>
  python worker.py --id w1 --coordinator 192.168.1.10
"""

import socket
import ssl
import threading
import time
import bisect
import sys
import os
import json
import argparse
import signal
import uuid
import logging
from urllib.parse import urlparse
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from protocol import (
    send_msg, recv_msg, ConnectionClosed, ProtocolError,
    Msg, MsgType,
    msg_register, msg_metrics, msg_report, msg_heartbeat, msg_error,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(threadName)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("worker")

# ═══════════════════════════════════════════════════════════════════
#  RAW HTTP ENGINE
# ═══════════════════════════════════════════════════════════════════

@dataclass
class HttpResult:
    status: int
    latency_ms: float
    error: Optional[str] = None

_ssl_ctx: Optional[ssl.SSLContext] = None

def _get_ssl():
    global _ssl_ctx
    if _ssl_ctx is None:
        _ssl_ctx = ssl.create_default_context()
    return _ssl_ctx

def _parse_url(url: str):
    p = urlparse(url)
    tls = p.scheme == "https"
    host = p.hostname or "localhost"
    port = p.port or (443 if tls else 80)
    path = (p.path or "/") + (f"?{p.query}" if p.query else "")
    return host, port, path, tls

def _build_request(method: str, host: str, path: str,
                   headers: dict, body: Optional[str]) -> bytes:
    lines = [
        f"{method} {path} HTTP/1.1",
        f"Host: {host}",
        "Connection: close",
        "User-Agent: DLT/1.0",
        "Accept: */*",
    ]
    for k, v in headers.items():
        if k.lower() not in ("host", "connection", "user-agent"):
            lines.append(f"{k}: {v}")
    if body:
        b = body.encode()
        lines.append(f"Content-Length: {len(b)}")
        if not any(k.lower() == "content-type" for k in headers):
            lines.append("Content-Type: application/json")
    else:
        b = b""
    return ("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + b

def _decode_chunked(data: bytes) -> bytes:
    out, off = bytearray(), 0
    while off < len(data):
        end = data.find(b"\r\n", off)
        if end < 0: break
        raw_size = data[off:end].decode("ascii").split(";")[0].strip()
        try: sz = int(raw_size, 16)
        except ValueError: break
        if sz == 0: break
        start = end + 2
        out.extend(data[start:start + sz])
        off = start + sz + 2
    return bytes(out)

def _parse_response(raw: bytes) -> int:
    sep = raw.find(b"\r\n\r\n")
    if sep < 0:
        raise ValueError("malformed response")
    hdr = raw[:sep].decode("ascii", errors="replace")
    parts = hdr.split("\r\n")[0].split(" ", 2)
    if len(parts) < 2:
        raise ValueError("bad status line")
    return int(parts[1])

def http_fire(method: str, url: str, headers: dict,
              body: Optional[str], timeout: float) -> HttpResult:
    """Execute one HTTP request over a raw TCP socket."""
    host, port, path, tls = _parse_url(url)
    sock = None
    t0 = time.perf_counter()
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        if tls:
            sock = _get_ssl().wrap_socket(sock, server_hostname=host)
        sock.sendall(_build_request(method, host, path, headers, body))
        chunks = []
        while True:
            try:
                c = sock.recv(8192)
                if not c: break
                chunks.append(c)
            except socket.timeout:
                break
        ms = (time.perf_counter() - t0) * 1000
        raw = b"".join(chunks)
        if not raw:
            return HttpResult(0, ms, "empty_response")
        return HttpResult(_parse_response(raw), ms)
    except socket.timeout:
        return HttpResult(0, (time.perf_counter() - t0) * 1000, "timeout")
    except ConnectionRefusedError:
        return HttpResult(0, (time.perf_counter() - t0) * 1000, "conn_refused")
    except Exception as e:
        return HttpResult(0, (time.perf_counter() - t0) * 1000, type(e).__name__)
    finally:
        if sock:
            try: sock.close()
            except Exception: pass

# ═══════════════════════════════════════════════════════════════════
#  METRICS TRACKER
# ═══════════════════════════════════════════════════════════════════

class Tracker:
    """Thread-safe latency + error tracker with percentile support."""

    def __init__(self):
        self._lock = threading.Lock()
        self._latencies: List[float] = []
        self._total = 0
        self._errors = 0
        self._err_detail: Dict[str, int] = defaultdict(int)
        self._win_count = 0
        self._win_start = time.time()

    def record(self, ms: float, err: Optional[str]):
        with self._lock:
            bisect.insort(self._latencies, ms)
            self._total += 1
            self._win_count += 1
            if err:
                self._errors += 1
                self._err_detail[err] += 1

    def snapshot_reset(self) -> dict:
        with self._lock:
            now = time.time()
            dt = max(now - self._win_start, 0.001)
            lats = self._latencies
            d = dict(
                rps=round(self._win_count / dt, 2),
                avg_ms=round(sum(lats)/len(lats), 2) if lats else 0,
                p50_ms=_pct(lats, 50), p95_ms=_pct(lats, 95),
                p99_ms=_pct(lats, 99),
                total=self._total, errors=self._errors,
                err_detail=dict(self._err_detail),
            )
            self._latencies = []
            self._win_count = 0
            self._win_start = now
            return d

    def summary(self) -> dict:
        with self._lock:
            return dict(total=self._total, errors=self._errors,
                        error_rate=round(self._errors/max(self._total,1)*100, 2),
                        err_detail=dict(self._err_detail))

def _pct(s: List[float], p: int) -> float:
    if not s: return 0.0
    return round(s[min(int(len(s) * p / 100), len(s) - 1)], 2)

# ═══════════════════════════════════════════════════════════════════
#  LOAD EXECUTOR
# ═══════════════════════════════════════════════════════════════════

class Executor:
    def __init__(self, url: str, method: str, headers: dict,
                 body: Optional[str], rps: int, vusers: int,
                 timeout: float):
        self.url = url
        self.method = method
        self.headers = headers
        self.body = body
        self._rps = rps
        self._vusers = vusers
        self._timeout = timeout
        self.tracker = Tracker()
        self._stop = threading.Event()
        self._rps_lock = threading.Lock()
        self._threads: List[threading.Thread] = []

    def update_rps(self, r: int):
        with self._rps_lock:
            self._rps = r

    def start(self):
        for i in range(self._vusers):
            t = threading.Thread(target=self._vu_loop, daemon=True,
                                 name=f"vu-{i}")
            self._threads.append(t); t.start()
        log.info(f"spawned {self._vusers} VUs, target {self._rps} RPS")

    def stop(self):
        self._stop.set()
        for t in self._threads:
            t.join(timeout=5)

    def _vu_loop(self):
        while not self._stop.is_set():
            with self._rps_lock:
                rps = self._rps
            interval = 1.0 / max(rps / max(self._vusers, 1), 0.1)
            t0 = time.perf_counter()
            res = http_fire(self.method, self.url, self.headers,
                            self.body, self._timeout)
            err = res.error if res.error else (
                f"http_{res.status}" if res.status >= 400 else None)
            self.tracker.record(res.latency_ms, err)
            left = interval - (time.perf_counter() - t0)
            if left > 0:
                self._stop.wait(left)

# ═══════════════════════════════════════════════════════════════════
#  WORKER CLIENT
# ═══════════════════════════════════════════════════════════════════

class Worker:
    def __init__(self, wid: str, host: str, port: int, token: str,
                 retries: int = 5, retry_delay: float = 2.0):
        self.wid = wid
        self.host = host
        self.port = port
        self.token = token
        self.retries = retries
        self.retry_delay = retry_delay
        self._sock: Optional[socket.socket] = None
        self._slock = threading.Lock()
        self._running = False
        self._stop = threading.Event()
        self._exec: Optional[Executor] = None
        self._cfg: Optional[dict] = None

    def run(self):
        self._running = True
        if not self._connect():
            return
        if not self._register():
            return
        threading.Thread(target=self._hb_loop, daemon=True, name="hb").start()
        self._rx_loop()

    def shutdown(self):
        log.info("shutting down worker")
        self._running = False
        self._stop.set()
        if self._exec: self._exec.stop()
        if self._sock:
            try: self._sock.close()
            except Exception: pass

    # ──── connection ────

    def _connect(self) -> bool:
        for attempt in range(1, self.retries + 1):
            try:
                log.info(f"connecting {self.host}:{self.port} "
                         f"(attempt {attempt}/{self.retries})")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(30.0)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s.connect((self.host, self.port))
                self._sock = s
                log.info("connected")
                return True
            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                log.warning(f"attempt {attempt} failed: {e}")
                if attempt < self.retries:
                    time.sleep(self.retry_delay)
        return False

    def _register(self) -> bool:
        try:
            self._send(msg_register(self.wid))
            raw = recv_msg(self._sock, self.token)
            m = Msg.decode(raw)
            if m.type != MsgType.REGISTER_ACK or not m.payload.get("accepted"):
                log.error(f"rejected: {m.payload.get('reason', '?')}")
                return False
            log.info(f"registered as '{self.wid}'")
            return True
        except (ConnectionClosed, ProtocolError) as e:
            log.error(f"register failed: {e}")
            return False

    # ──── rx loop ────

    def _rx_loop(self):
        while self._running:
            try:
                self._sock.settimeout(30.0)
                raw = recv_msg(self._sock, self.token)
                m = Msg.decode(raw)
                self._dispatch(m)
            except (ConnectionClosed, ProtocolError) as e:
                if self._running:
                    log.error(f"lost coordinator: {e}")
                break
            except socket.timeout:
                continue
        self.shutdown()

    def _dispatch(self, m: Msg):
        t = m.type
        if   t == MsgType.CONFIG:       self._on_config(m)
        elif t == MsgType.START:        self._on_start()
        elif t == MsgType.STOP:         self._on_stop()
        elif t == MsgType.HEARTBEAT:    self._send(msg_heartbeat(self.wid))
        elif t == MsgType.HEARTBEAT_ACK: pass
        elif t == MsgType.RAMP_UPDATE:  self._on_ramp(m)
        else: log.warning(f"unhandled: {t}")

    def _on_config(self, m: Msg):
        self._cfg = m.payload
        log.info(f"config: {self._cfg.get('rps')} RPS, "
                 f"{self._cfg.get('vusers')} VUs, "
                 f"{self._cfg.get('duration')}s → {self._cfg.get('target_url')}")

    def _on_start(self):
        if not self._cfg:
            self._send(msg_error(self.wid, "no config")); return
        c = self._cfg
        self._exec = Executor(
            c["target_url"], c.get("method", "GET"),
            c.get("headers", {}), c.get("body"),
            c["rps"], c["vusers"], c.get("timeout_s", 10.0),
        )
        self._exec.start()
        threading.Thread(target=self._metric_loop, args=(c["duration"],),
                         daemon=True, name="metrics").start()
        log.info("test STARTED")

    def _on_stop(self):
        log.info("STOP received")
        self._stop.set()
        if self._exec:
            self._exec.stop()
            try:
                self._send(msg_report(self.wid, self._exec.tracker.summary()))
            except Exception:
                pass

    def _on_ramp(self, m: Msg):
        r = m.payload.get("rps", 0)
        if self._exec and r > 0:
            self._exec.update_rps(r)
            log.info(f"ramp → {r} RPS")

    # ──── metrics reporter ────

    def _metric_loop(self, dur: int):
        t0 = time.time()
        while not self._stop.is_set():
            self._stop.wait(1.0)
            if not self._exec: break
            if time.time() - t0 >= dur:
                break
            s = self._exec.tracker.snapshot_reset()
            try:
                self._send(msg_metrics(self.wid, **s))
            except Exception:
                break
        # final
        if self._exec:
            self._exec.stop()
            try:
                self._send(msg_report(self.wid, self._exec.tracker.summary()))
            except Exception:
                pass

    # ──── heartbeat ────

    def _hb_loop(self):
        while self._running:
            self._stop.wait(5.0)
            if not self._running: break
            try:
                self._send(msg_heartbeat(self.wid))
            except Exception:
                break

    # ──── send helper ────

    def _send(self, m: Msg):
        with self._slock:
            send_msg(self._sock, m.encode(), self.token)

# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="Load Tester — Worker")
    ap.add_argument("--id", default="", help="worker id (auto if empty)")
    ap.add_argument("--coordinator", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9500)
    a = ap.parse_args()

    token = os.environ.get("LOAD_TEST_TOKEN", "")
    if not token:
        print("set LOAD_TEST_TOKEN"); sys.exit(1)

    wid = a.id or f"w-{uuid.uuid4().hex[:8]}"
    w = Worker(wid, a.coordinator, a.port, token)

    def _sig(s, f):
        w.shutdown(); sys.exit(0)
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    w.run()


if __name__ == "__main__":
    main()