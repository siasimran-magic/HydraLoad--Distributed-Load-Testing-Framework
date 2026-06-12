"""protocol.py

This file defines the *entire* coordinator ↔ worker communication protocol.

In simple terms, this project runs a distributed load test:
    - The **Coordinator** (server) listens for worker connections.
    - Each **Worker** (client) connects, registers itself, receives test config,
        starts generating HTTP load, and periodically sends metrics back.

This module is the "single source of truth" for:
    1) The wire framing (how a message is chopped into bytes on a TCP stream)
    2) Authentication/integrity (HMAC signature using a shared token)
    3) Message schemas / message type names

Wire format (bytes on the TCP stream):
    [4 bytes: payload length, big-endian]
    [64 bytes: HMAC-SHA256 hexdigest of payload, ASCII]
    [payload bytes: UTF-8 JSON string]

Why the HMAC exists:
    - Prevents random processes from injecting fake messages.
    - Detects corruption/tampering: if signature doesn't match, the message is rejected.

Expected high-level message flow:
    1) Worker -> Coordinator: REGISTER(worker_id)
    2) Coordinator -> Worker: REGISTER_ACK(accepted/denied)
    3) Coordinator -> Worker: CONFIG(test parameters + assigned share)
    4) Coordinator -> Worker: START
    5) Worker -> Coordinator: METRICS (every ~1s)
    6) Coordinator -> Worker: STOP (or timer triggers end)
    7) Worker -> Coordinator: REPORT (final summary)

Heartbeats are exchanged in the background to detect dead connections.
"""

import hmac
import hashlib
import struct
import socket
import json
import time
import secrets
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional

# ──────────────────────────── Constants ────────────────────────────

MAX_MESSAGE_SIZE = 1 << 20  # 1 MB
HEADER_SIZE = 4
SIGNATURE_SIZE = 64

# ──────────────────────────── Crypto ──────────────────────────────

def generate_token() -> str:
    """Create a random shared secret used to authenticate the whole session.

    Both coordinator and workers must use the same token (via env var
    LOAD_TEST_TOKEN). If tokens differ, every message will fail HMAC verification.
    """
    return secrets.token_hex(32)

def _sign(payload: bytes, token: str) -> str:
    # HMAC-SHA256(payload, token) as a hex string (64 ASCII chars).
    return hmac.new(token.encode(), payload, hashlib.sha256).hexdigest()

def _verify(payload: bytes, signature: str, token: str) -> bool:
    # Constant-time comparison to avoid timing leaks.
    return hmac.compare_digest(_sign(payload, token), signature)

# ──────────────────────────── Framing ─────────────────────────────

class ProtocolError(Exception):
    """Any wire-level violation."""

class ConnectionClosed(ProtocolError):
    """Remote hung up."""

def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes from the TCP stream or raise ConnectionClosed.

    TCP is a stream: one send on the other side may arrive in many chunks.
    This helper loops until we have the required number of bytes.
    """
    buf = bytearray()
    while len(buf) < n:
        try:
            chunk = sock.recv(min(n - len(buf), 8192))
        except (ConnectionResetError, BrokenPipeError, OSError):
            raise ConnectionClosed("connection reset")
        if not chunk:
            raise ConnectionClosed("connection closed")
        buf.extend(chunk)
    return bytes(buf)

def send_msg(sock: socket.socket, data: str, token: str) -> None:
    """Send one authenticated message (JSON string) over the socket.

    Called by:
      - Coordinator when sending CONFIG/START/STOP/RAMP_UPDATE/etc.
      - Worker when sending REGISTER/METRICS/REPORT/etc.
    """
    payload = data.encode()
    if len(payload) > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"message too large: {len(payload)}")
    sig = _sign(payload, token).encode("ascii")
    try:
        sock.sendall(struct.pack("!I", len(payload)) + sig + payload)
    except (ConnectionResetError, BrokenPipeError, OSError) as e:
        raise ConnectionClosed(str(e))

def recv_msg(sock: socket.socket, token: str) -> str:
    """Receive one authenticated message and return its JSON string payload."""
    header = _recv_exact(sock, HEADER_SIZE)
    length = struct.unpack("!I", header)[0]
    if length == 0 or length > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"invalid payload length: {length}")
    sig = _recv_exact(sock, SIGNATURE_SIZE).decode("ascii")
    payload = _recv_exact(sock, length)
    if not _verify(payload, sig, token):
        raise ProtocolError("HMAC verification failed")
    return payload.decode()

# ──────────────────────────── Messages ────────────────────────────

class MsgType(str, Enum):
    """All message type names exchanged between coordinator and workers."""

    # Registration / control plane
    REGISTER     = "REGISTER"
    REGISTER_ACK = "REGISTER_ACK"
    CONFIG       = "CONFIG"
    START        = "START"
    STOP         = "STOP"

    # Metrics / reporting plane
    METRICS      = "METRICS"
    REPORT       = "REPORT"

    # Liveness / orchestration
    HEARTBEAT    = "HEARTBEAT"
    HEARTBEAT_ACK = "HEARTBEAT_ACK"
    RAMP_UPDATE  = "RAMP_UPDATE"

    # Error propagation (worker -> coordinator)
    ERROR        = "ERROR"

_VALID_TYPES = {e.value for e in MsgType}

@dataclass
class Msg:
    """Typed message envelope.

    `payload` is message-specific data (dict).
    `ts` is a sender-side timestamp (used for plotting; not trusted for security).
    `sender` is informational (worker id or "coordinator").
    """
    type: str
    payload: Dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)
    sender: str = ""

    def encode(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def decode(cls, raw: str) -> "Msg":
        d = json.loads(raw)
        if d.get("type") not in _VALID_TYPES:
            raise ValueError(f"unknown message type: {d.get('type')}")
        return cls(d["type"], d.get("payload", {}),
                   d.get("ts", time.time()), d.get("sender", ""))

# ────────────────── Convenience constructors ──────────────────────

def msg_register(wid: str) -> Msg:
    return Msg(MsgType.REGISTER, {"worker_id": wid}, sender=wid)

def msg_register_ack(ok: bool, reason: str = "") -> Msg:
    return Msg(MsgType.REGISTER_ACK, {"accepted": ok, "reason": reason},
               sender="coordinator")

def msg_config(cfg: dict) -> Msg:
    return Msg(MsgType.CONFIG, cfg, sender="coordinator")

def msg_start() -> Msg:
    return Msg(MsgType.START, sender="coordinator")

def msg_stop() -> Msg:
    return Msg(MsgType.STOP, sender="coordinator")

def msg_metrics(wid: str, **kw) -> Msg:
    kw["worker_id"] = wid
    return Msg(MsgType.METRICS, kw, sender=wid)

def msg_report(wid: str, summary: dict) -> Msg:
    return Msg(MsgType.REPORT, {"worker_id": wid, "summary": summary}, sender=wid)

def msg_heartbeat(sender: str) -> Msg:
    return Msg(MsgType.HEARTBEAT, sender=sender)

def msg_heartbeat_ack() -> Msg:
    return Msg(MsgType.HEARTBEAT_ACK, sender="coordinator")

def msg_ramp(new_rps: int) -> Msg:
    return Msg(MsgType.RAMP_UPDATE, {"rps": new_rps}, sender="coordinator")

def msg_error(sender: str, err: str) -> Msg:
    return Msg(MsgType.ERROR, {"error": err}, sender=sender)