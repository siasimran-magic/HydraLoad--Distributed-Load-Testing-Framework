#!/usr/bin/env python3
"""
Coordinator — accepts workers, distributes config, orchestrates test,
aggregates live metrics, renders dashboard, generates HTML report.

Usage on Windows:
  set LOAD_TEST_TOKEN=<token>
  python coordinator.py --target http://127.0.0.1:8080/ --rps 100 --duration 30 --min-workers 2
"""

import socket
import threading
import time
import sys
import os
import json
import argparse
import signal
import copy
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from protocol import (
    send_msg, recv_msg, ConnectionClosed, ProtocolError, generate_token,
    Msg, MsgType,
    msg_register_ack, msg_config, msg_start, msg_stop,
    msg_heartbeat, msg_heartbeat_ack, msg_ramp,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(threadName)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("coord")

# ═══════════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════

@dataclass
class WorkerHandle:
    wid: str
    sock: socket.socket
    addr: tuple
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_hb: float = field(default_factory=time.time)
    alive: bool = True
    assigned_rps: int = 0
    assigned_vu: int = 0


@dataclass
class Snapshot:
    wid: str
    ts: float
    rps: float = 0
    avg_ms: float = 0
    p50_ms: float = 0
    p95_ms: float = 0
    p99_ms: float = 0
    total: int = 0
    errors: int = 0
    err_detail: dict = field(default_factory=dict)


@dataclass
class Aggregate:
    ts: float
    n_workers: int
    rps: float
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    total: int
    errors: int
    per_worker: Dict[str, Snapshot] = field(default_factory=dict)

# ═══════════════════════════════════════════════════════════════════
#  AGGREGATOR
# ═══════════════════════════════════════════════════════════════════

class Aggregator:
    def __init__(self):
        self._lock = threading.RLock()
        self._latest: Dict[str, Snapshot] = {}
        self._history: List[Aggregate] = []
        self._start: Optional[float] = None
        self._dur = 0

    def set_test(self, start: float, dur: int):
        with self._lock:
            self._start = start
            self._dur = dur

    def record(self, s: Snapshot):
        with self._lock:
            self._latest[s.wid] = s

    def drop(self, wid: str):
        with self._lock:
            self._latest.pop(wid, None)

    @property
    def elapsed(self) -> float:
        return (time.time() - self._start) if self._start else 0.0

    @property
    def duration(self) -> int:
        return self._dur

    def aggregate(self) -> Aggregate:
        with self._lock:
            ws = list(self._latest.values())
            if not ws:
                return Aggregate(time.time(), 0, 0, 0, 0, 0, 0, 0, 0)
            tw = sum(w.rps for w in ws) or 1.0
            agg = Aggregate(
                ts=time.time(),
                n_workers=len(ws),
                rps=round(sum(w.rps for w in ws), 1),
                avg_ms=round(sum(w.avg_ms * w.rps for w in ws) / tw, 1),
                p50_ms=round(max(w.p50_ms for w in ws), 1),
                p95_ms=round(max(w.p95_ms for w in ws), 1),
                p99_ms=round(max(w.p99_ms for w in ws), 1),
                total=sum(w.total for w in ws),
                errors=sum(w.errors for w in ws),
                per_worker={w.wid: copy.copy(w) for w in ws},
            )
            self._history.append(agg)
            return agg

    def get_history(self) -> List[Aggregate]:
        with self._lock:
            return list(self._history)

# ═══════════════════════════════════════════════════════════════════
#  DASHBOARD
# ═══════════════════════════════════════════════════════════════════

def render_dashboard(agg: Aggregate, target: str, elapsed: float, dur: int):
    os.system("cls" if os.name == "nt" else "clear")
    em, es = divmod(int(elapsed), 60)
    dm, ds = divmod(dur, 60)
    print("=" * 62)
    print("        DISTRIBUTED LOAD TEST — LIVE DASHBOARD")
    print("=" * 62)
    print(f"  Target:   {target}")
    print(f"  Workers:  {agg.n_workers} connected")
    print(f"  Elapsed:  {em:02d}:{es:02d} / {dm:02d}:{ds:02d}")
    print()
    hdr = f"{'WORKER':<14}{'RPS':>8}{'AVG':>9}{'P50':>9}{'P95':>9}{'P99':>9}{'ERR':>7}"
    print(hdr)
    print("─" * len(hdr))
    for wid in sorted(agg.per_worker):
        w = agg.per_worker[wid]
        label = wid[:12]
        print(f"{label:<14}{w.rps:>8.1f}{w.avg_ms:>7.0f}ms{w.p50_ms:>7.0f}ms"
              f"{w.p95_ms:>7.0f}ms{w.p99_ms:>7.0f}ms{w.errors:>7}")
    print("─" * len(hdr))
    print(f"{'TOTAL':<14}{agg.rps:>8.1f}{agg.avg_ms:>7.0f}ms{agg.p50_ms:>7.0f}ms"
          f"{agg.p95_ms:>7.0f}ms{agg.p99_ms:>7.0f}ms{agg.errors:>7}")
    print()
    if dur > 0:
        pct = min(elapsed / dur, 1.0)
        bw = 40
        print(f"  [{'█' * int(bw * pct)}{'·' * (bw - int(bw * pct))}] {pct*100:.0f}%")
    print(f"\n  Total requests: {agg.total:,}")
    sys.stdout.flush()

# ═══════════════════════════════════════════════════════════════════
#  HTML REPORT
# ═══════════════════════════════════════════════════════════════════

def generate_report(history: List[Aggregate], target: str,
                    path: str = "report.html"):
    if not history:
        with open(path, "w") as f:
            f.write("<h1>No data</h1>")
        return path

    t0 = history[0].ts
    labels = [f"{h.ts - t0:.0f}s" for h in history]
    rps   = [h.rps for h in history]
    avg   = [h.avg_ms for h in history]
    p95   = [h.p95_ms for h in history]
    p99   = [h.p99_ms for h in history]
    errs  = [history[0].errors] + [
        max(0, history[i].errors - history[i-1].errors)
        for i in range(1, len(history))
    ]
    fin = history[-1]
    err_rate = round(fin.errors / max(fin.total, 1) * 100, 2)

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Load Test Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
body{{font-family:system-ui;max-width:1000px;margin:auto;padding:20px;background:#f5f5f5}}
.hdr{{background:#1a1a2e;color:#fff;padding:24px;border-radius:8px;margin-bottom:16px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px}}
.card{{background:#fff;border-radius:8px;padding:16px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.1)}}
.val{{font-size:28px;font-weight:700;color:#1a1a2e}}.lbl{{font-size:11px;color:#888;text-transform:uppercase}}
.chart-box{{background:#fff;border-radius:8px;padding:16px;margin-bottom:12px;box-shadow:0 1px 3px rgba(0,0,0,.1)}}
</style></head><body>
<div class="hdr"><h2 style="margin:0">Load Test Report</h2>
<p style="margin:4px 0 0;opacity:.8">{target}</p>
<p style="margin:2px 0 0;opacity:.6">{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</p></div>
<div class="grid">
<div class="card"><div class="val">{fin.total:,}</div><div class="lbl">Requests</div></div>
<div class="card"><div class="val">{max(rps):.0f}</div><div class="lbl">Peak RPS</div></div>
<div class="card"><div class="val">{fin.avg_ms:.0f}ms</div><div class="lbl">Avg Latency</div></div>
<div class="card"><div class="val">{fin.p95_ms:.0f}ms</div><div class="lbl">P95</div></div>
<div class="card"><div class="val">{err_rate}%</div><div class="lbl">Error Rate</div></div>
<div class="card"><div class="val">{fin.n_workers}</div><div class="lbl">Workers</div></div>
</div>
<div class="chart-box"><canvas id="c1" height="70"></canvas></div>
<div class="chart-box"><canvas id="c2" height="70"></canvas></div>
<div class="chart-box"><canvas id="c3" height="50"></canvas></div>
<script>
const L={json.dumps(labels)},R={json.dumps(rps)},A={json.dumps(avg)},
P95={json.dumps(p95)},P99={json.dumps(p99)},E={json.dumps(errs)};
const line=(id,ds,t)=>new Chart(document.getElementById(id),
{{type:'line',data:{{labels:L,datasets:ds}},options:{{responsive:true,
plugins:{{title:{{display:true,text:t}}}},elements:{{point:{{radius:0}}}},
scales:{{y:{{beginAtZero:true}}}}}}}});
line('c1',[{{label:'RPS',data:R,borderColor:'#3498db',fill:true,
backgroundColor:'rgba(52,152,219,.1)',tension:.3}}],'Throughput');
line('c2',[{{label:'Avg',data:A,borderColor:'#27ae60',tension:.3}},
{{label:'P95',data:P95,borderColor:'#f39c12',tension:.3}},
{{label:'P99',data:P99,borderColor:'#e74c3c',tension:.3}}],'Latency (ms)');
new Chart(document.getElementById('c3'),{{type:'bar',data:{{labels:L,
datasets:[{{label:'Errors',data:E,backgroundColor:'rgba(231,76,60,.5)'}}]}},
options:{{responsive:true,plugins:{{title:{{display:true,text:'Errors / Interval'}}}},
scales:{{y:{{beginAtZero:true}}}}}}}});
</script></body></html>"""

    with open(path, "w") as f:
        f.write(html)
    return path

# ═══════════════════════════════════════════════════════════════════
#  COORDINATOR SERVER
# ═══════════════════════════════════════════════════════════════════

class Coordinator:
    def __init__(self, host: str, port: int, token: str, test_cfg: dict,
                 max_workers: int = 20, hb_interval: float = 3.0,
                 hb_timeout: float = 10.0):
        self.host = host
        self.port = port
        self.token = token
        self.test_cfg = test_cfg        # target_url, method, headers, body,
                                        # rps, duration, vusers, ramp_up_s, timeout_s
        self.max_workers = max_workers
        self.hb_interval = hb_interval
        self.hb_timeout = hb_timeout

        self._workers: Dict[str, WorkerHandle] = {}
        self._wlock = threading.RLock()
        self._agg = Aggregator()
        self._running = False
        self._test_active = False
        self._done = threading.Event()
        self._shutdown = threading.Event()
        self._reports: Dict[str, dict] = {}
        self._rlock = threading.Lock()
        self._ssock: Optional[socket.socket] = None

    # ────────────── public api ──────────────

    def run(self, min_workers: int = 1):
        self._bind()
        self._running = True

        threading.Thread(target=self._accept_loop, daemon=True, name="accept").start()
        threading.Thread(target=self._hb_loop, daemon=True, name="hb").start()

        log.info(f"listening on {self.host}:{self.port}  (need {min_workers} workers)")

        # wait for enough workers
        while self._running:
            with self._wlock:
                n = sum(1 for w in self._workers.values() if w.alive)
            if n >= min_workers:
                break
            time.sleep(0.5)

        if not self._running:
            return

        log.info(f"{min_workers} worker(s) connected — starting in 2s")
        time.sleep(2)

        self._distribute_config()
        time.sleep(0.5)
        self._start_test()

        # dashboard loop (main thread)
        threading.Thread(target=self._ramp_loop, daemon=True, name="ramp").start()
        threading.Thread(target=self._auto_stop, daemon=True, name="timer").start()

        while not self._done.is_set():
            a = self._agg.aggregate()
            render_dashboard(a, self.test_cfg["target_url"],
                             self._agg.elapsed, self._agg.duration)
            self._done.wait(1.0)

        # stop
        self._send_stop()
        log.info("waiting 5s for final reports…")
        time.sleep(5)

        rpath = generate_report(self._agg.get_history(),
                                self.test_cfg["target_url"])
        log.info(f"report saved → {rpath}")
        self.shutdown()

    def shutdown(self):
        self._running = False
        self._shutdown.set()
        self._done.set()
        with self._wlock:
            for w in self._workers.values():
                try: w.sock.close()
                except Exception: pass
        if self._ssock:
            try: self._ssock.close()
            except Exception: pass
        log.info("coordinator stopped")

    # ────────────── socket setup ──────────────

    def _bind(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.settimeout(1.0)
        s.bind((self.host, self.port))
        s.listen(self.max_workers)
        self._ssock = s

    # ────────────── accept / register ──────────────

    def _accept_loop(self):
        while self._running:
            try:
                cs, addr = self._ssock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            cs.settimeout(30.0)
            threading.Thread(target=self._do_register, args=(cs, addr),
                             daemon=True, name=f"reg-{addr[1]}").start()

    def _do_register(self, sock, addr):
        try:
            raw = recv_msg(sock, self.token)
            m = Msg.decode(raw)
            if m.type != MsgType.REGISTER:
                self._raw_send(sock, msg_register_ack(False, "expected REGISTER"))
                sock.close(); return

            wid = m.payload.get("worker_id", "")
            with self._wlock:
                if not wid or wid in self._workers or len(self._workers) >= self.max_workers:
                    reason = "dup id" if wid in self._workers else "limit" if not wid else "no id"
                    self._raw_send(sock, msg_register_ack(False, reason))
                    sock.close(); return
                self._workers[wid] = WorkerHandle(wid, sock, addr)

            self._send_to(wid, msg_register_ack(True))
            log.info(f"worker '{wid}' registered ({addr[0]})")
            threading.Thread(target=self._recv_loop, args=(wid,),
                             daemon=True, name=f"rx-{wid}").start()
        except (ConnectionClosed, ProtocolError, Exception) as e:
            log.warning(f"register failed {addr}: {e}")
            try: sock.close()
            except Exception: pass

    # ────────────── per-worker receiver ──────────────

    def _recv_loop(self, wid: str):
        while self._running:
            wh = self._workers.get(wid)
            if not wh or not wh.alive:
                break
            try:
                wh.sock.settimeout(self.hb_timeout * 2)
                raw = recv_msg(wh.sock, self.token)
                m = Msg.decode(raw)
                wh.last_hb = time.time()
                self._dispatch(wid, m)
            except (ConnectionClosed, ProtocolError):
                self._worker_died(wid); break
            except socket.timeout:
                continue
            except Exception as e:
                log.error(f"rx error {wid}: {e}")
                self._worker_died(wid); break

    def _dispatch(self, wid: str, m: Msg):
        if m.type == MsgType.METRICS:
            p = m.payload
            self._agg.record(Snapshot(
                wid=wid, ts=m.ts,
                rps=p.get("rps", 0), avg_ms=p.get("avg_ms", 0),
                p50_ms=p.get("p50_ms", 0), p95_ms=p.get("p95_ms", 0),
                p99_ms=p.get("p99_ms", 0), total=p.get("total", 0),
                errors=p.get("errors", 0), err_detail=p.get("err_detail", {}),
            ))
        elif m.type == MsgType.REPORT:
            with self._rlock:
                self._reports[wid] = m.payload.get("summary", {})
            log.info(f"final report from '{wid}'")
            with self._wlock:
                alive = {w for w, h in self._workers.items() if h.alive}
            with self._rlock:
                if alive <= set(self._reports):
                    self._done.set()
        elif m.type == MsgType.HEARTBEAT:
            self._send_to(wid, msg_heartbeat_ack())
        elif m.type == MsgType.ERROR:
            log.error(f"worker '{wid}': {m.payload.get('error')}")

    # ────────────── failure / redistribution ──────────────

    def _worker_died(self, wid: str):
        with self._wlock:
            wh = self._workers.get(wid)
            if wh:
                wh.alive = False
                try: wh.sock.close()
                except Exception: pass
        self._agg.drop(wid)
        log.warning(f"worker '{wid}' lost")
        if self._test_active:
            self._redistribute()

    def _redistribute(self):
        with self._wlock:
            alive = [w for w in self._workers.values() if w.alive]
        if not alive:
            log.error("all workers dead"); self._done.set(); return
        total_rps = self.test_cfg["rps"]
        per = total_rps // len(alive)
        rem = total_rps % len(alive)
        for i, wh in enumerate(alive):
            r = per + (1 if i < rem else 0)
            wh.assigned_rps = r
            self._send_to(wh.wid, msg_ramp(r))
        log.info(f"redistributed {total_rps} RPS across {len(alive)} workers")

    # ────────────── heartbeat ──────────────

    def _hb_loop(self):
        while self._running:
            self._shutdown.wait(self.hb_interval)
            if not self._running: break
            now = time.time()
            with self._wlock:
                for wid, wh in list(self._workers.items()):
                    if not wh.alive: continue
                    if now - wh.last_hb > self.hb_timeout:
                        log.warning(f"heartbeat timeout: '{wid}'")
                        self._worker_died(wid); continue
                    self._send_to(wid, msg_heartbeat("coordinator"))

    # ────────────── config / start / stop / ramp ──────────────

    def _distribute_config(self):
        with self._wlock:
            alive = [w for w in self._workers.values() if w.alive]
        n = len(alive)
        if n == 0:
            log.error("no workers"); return
        rps_each = self.test_cfg["rps"] // n
        vu_each  = max(1, self.test_cfg["vusers"] // n)
        rps_rem  = self.test_cfg["rps"] % n
        vu_rem   = self.test_cfg["vusers"] % n
        for i, wh in enumerate(alive):
            r = rps_each + (1 if i < rps_rem else 0)
            v = vu_each  + (1 if i < vu_rem  else 0)
            wh.assigned_rps = r; wh.assigned_vu = v
            self._send_to(wh.wid, msg_config({
                "target_url": self.test_cfg["target_url"],
                "method": self.test_cfg.get("method", "GET"),
                "headers": self.test_cfg.get("headers", {}),
                "body": self.test_cfg.get("body"),
                "rps": r, "vusers": v,
                "duration": self.test_cfg["duration"],
                "timeout_s": self.test_cfg.get("timeout_s", 10.0),
            }))
        log.info(f"config sent to {n} workers")

    def _start_test(self):
        self._test_active = True
        self._agg.set_test(time.time(), self.test_cfg["duration"])
        self._broadcast(msg_start())
        log.info("test STARTED")

    def _send_stop(self):
        self._test_active = False
        self._broadcast(msg_stop())
        log.info("STOP sent")

    def _auto_stop(self):
        self._shutdown.wait(self.test_cfg["duration"])
        if self._running:
            self._done.set()

    def _ramp_loop(self):
        ramp_s = self.test_cfg.get("ramp_up_s", 0)
        if ramp_s <= 0: return
        steps = self.test_cfg.get("ramp_steps", 5)
        target = self.test_cfg["rps"]
        step_dur = ramp_s / steps
        for s in range(1, steps + 1):
            if not self._running or not self._test_active: break
            cur = int(target * s / steps)
            with self._wlock:
                alive = [w for w in self._workers.values() if w.alive]
            if not alive: break
            per = cur // len(alive); rem = cur % len(alive)
            for i, wh in enumerate(alive):
                r = per + (1 if i < rem else 0)
                wh.assigned_rps = r
                self._send_to(wh.wid, msg_ramp(r))
            log.info(f"ramp {s}/{steps}: {cur} RPS")
            self._shutdown.wait(step_dur)

    # ────────────── send helpers ──────────────

    def _send_to(self, wid: str, m: Msg):
        wh = self._workers.get(wid)
        if not wh or not wh.alive: return
        try:
            with wh.lock:
                send_msg(wh.sock, m.encode(), self.token)
        except (ConnectionClosed, ProtocolError):
            self._worker_died(wid)

    def _broadcast(self, m: Msg):
        with self._wlock:
            wids = [w for w, h in self._workers.items() if h.alive]
        for wid in wids:
            self._send_to(wid, m)

    def _raw_send(self, sock, m: Msg):
        try: send_msg(sock, m.encode(), self.token)
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="Load Tester — Coordinator")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9500)
    ap.add_argument("--target", default="", help="URL to test")
    ap.add_argument("--method", default="GET")
    ap.add_argument("--rps", type=int, default=100)
    ap.add_argument("--duration", type=int, default=30)
    ap.add_argument("--vusers", type=int, default=10)
    ap.add_argument("--ramp-up", type=int, default=0, dest="ramp_up_s")
    ap.add_argument("--ramp-steps", type=int, default=5)
    ap.add_argument("--min-workers", type=int, default=1)
    ap.add_argument("--max-workers", type=int, default=20)
    ap.add_argument("--gen-token", action="store_true",
                    help="print a new token and exit")
    a = ap.parse_args()

    # Generate token block updated for Windows CMD/PowerShell users
    if a.gen_token:
        t = generate_token()
        print("\n--- TOKEN GENERATED ---")
        print("Run this command in EVERY terminal before starting the scripts:")
        print(f"\nIf using CMD (Command Prompt):\n  set LOAD_TEST_TOKEN={t}")
        print(f"\nIf using PowerShell:\n  $env:LOAD_TEST_TOKEN=\"{t}\"\n")
        return

    # Enforce token existence
    token = os.environ.get("LOAD_TEST_TOKEN", "")
    if not token:
        print("Error: LOAD_TEST_TOKEN environment variable is not set.")
        print("Run 'python coordinator.py --gen-token' to create one.")
        sys.exit(1)

    # Enforce target existence
    if not a.target:
        print("Error: --target is required to run a test.")
        print("Example: python coordinator.py --target http://127.0.0.1:8080/ --rps 100")
        sys.exit(1)

    test_cfg = dict(
        target_url=a.target, method=a.method, rps=a.rps,
        duration=a.duration, vusers=a.vusers,
        ramp_up_s=a.ramp_up_s, ramp_steps=a.ramp_steps,
    )

    coord = Coordinator(a.host, a.port, token, test_cfg,
                        max_workers=a.max_workers)

    def _sig(s, f):
        coord.shutdown(); sys.exit(0)
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    coord.run(min_workers=a.min_workers)


if __name__ == "__main__":
    main()