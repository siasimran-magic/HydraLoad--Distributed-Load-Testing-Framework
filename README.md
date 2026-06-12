# HydraLoad

A secure distributed load testing framework built from scratch in Python.

HydraLoad distributes HTTP load generation across multiple worker nodes, aggregates metrics in real time, monitors worker health, and generates an interactive HTML performance report.

---

## What It Does

- Distributed load generation using multiple workers
- Coordinator-worker architecture
- Custom TCP communication protocol
- HMAC-authenticated messaging
- Real-time metrics aggregation
- Heartbeat-based worker monitoring
- Automatic load redistribution on worker failure
- Live terminal dashboard
- HTML report generation with performance charts

---

## Architecture

<img width="1536" height="1024" alt="CN" src="https://github.com/user-attachments/assets/5ebe0e14-72c9-4a49-aa45-078c3a3528fc" />

---

## Flow

```text
1. Workers connect to Coordinator
2. Coordinator validates and registers workers
3. Coordinator sends test configuration
4. Workers start generating HTTP requests
5. Target server responds
6. Workers send metrics every second
7. Coordinator aggregates metrics
8. Live dashboard is displayed
9. Test ends
10. HTML report is generated
```

### Message Flow

```text
Worker                  Coordinator                 Target

REGISTER      ────────►
REGISTER_ACK  ◄────────

CONFIG        ◄────────
START         ◄────────

HTTP Requests ───────────────────────────────────►
Responses     ◄───────────────────────────────────

METRICS       ────────►
HEARTBEAT     ◄───────►

STOP          ◄────────

REPORT        ────────►
```

---

## How To Run

### 1. Generate Token

```bash
python coordinator.py --gen-token
```

### 2. Set Token

#### Windows CMD

```cmd
set LOAD_TEST_TOKEN=<generated_token>
```

---

### 3. Start Target Server

```bash
python dummy_server.py
```

---

### 4. Start Coordinator

```bash
python coordinator.py --target http://127.0.0.1:8080/ --rps 100 --duration 30 --min-workers 2
```

---

### 5. Start Worker A

```bash
python worker.py --id worker-A
```

---

### 6. Start Worker B

```bash
python worker.py --id worker-B
```

---

## Output

### Live Dashboard

Displays:

- Requests Per Second (RPS)
- Average Latency
- P50 / P95 / P99 Latencies
- Error Counts
- Worker Status

### HTML Report

Automatically generates:

```text
report.html
```

Includes:

- Throughput Graph
- Latency Graphs
- Error Distribution
- Test Summary
- Worker Statistics

---

## Tech Stack

- Python
- TCP Sockets
- Multithreading
- HMAC-SHA256
- Chart.js
- HTML/CSS

---
