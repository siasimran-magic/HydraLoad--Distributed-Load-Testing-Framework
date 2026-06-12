# HydraLoad

> A Secure Distributed Load Testing Framework Built from Scratch in Python

HydraLoad is a distributed load testing framework that allows multiple worker nodes to generate traffic against a target server while a central coordinator orchestrates the test, aggregates metrics, monitors worker health, and generates a detailed HTML performance report.

Unlike traditional load-testing tools, HydraLoad implements its own communication protocol, authentication layer, metrics aggregation engine, heartbeat system, and reporting pipeline.

---

## Overview

HydraLoad follows a Coordinator–Worker architecture.

The **Coordinator** acts as the control plane:

- Accepts worker registrations
- Distributes test configurations
- Starts and stops tests
- Monitors worker health
- Aggregates live metrics
- Generates reports

The **Workers** act as load generators:

- Receive configuration from the coordinator
- Generate HTTP traffic
- Measure latency and throughput
- Send metrics periodically
- Submit final summaries

---

## Architecture

<p align="center">
  <img src="docs/architecture.png" alt="HydraLoad Architecture" width="100%">
</p>

---

## High-Level Workflow

### 1. Registration

Workers connect to the coordinator and register themselves.

```text
Worker ───── REGISTER ─────► Coordinator
Worker ◄── REGISTER_ACK ─── Coordinator
```

### 2. Configuration

Coordinator distributes test parameters.

```text
Coordinator ─── CONFIG ───► Worker
```

Parameters include:

- Target URL
- Duration
- Assigned RPS
- Virtual Users
- Request Method
- Headers
- Timeout

### 3. Start Test

Coordinator broadcasts a start signal.

```text
Coordinator ─── START ───► Workers
```

### 4. Load Generation

Workers generate HTTP requests against the target server.

```text
Workers ─── HTTP Requests ───► Target
Target ─── Responses ───► Workers
```

### 5. Metrics Aggregation

Workers report metrics every second.

```text
Workers ─── METRICS ───► Coordinator
```

Metrics include:

- Requests Per Second
- Average Latency
- P50 Latency
- P95 Latency
- P99 Latency
- Error Count
- Error Types

### 6. Health Monitoring

Heartbeats continuously verify worker liveness.

```text
Coordinator ◄──► Workers
```

### 7. Reporting

At the end of the test:

```text
Workers ─── REPORT ───► Coordinator
```

Coordinator generates:

- Live Dashboard
- Final HTML Report

---

# Features

### Distributed Load Generation

Generate traffic from multiple worker nodes simultaneously.

### Custom Protocol

Built a complete TCP-based messaging protocol from scratch.

### HMAC Authentication

Every message is signed using HMAC-SHA256 to prevent tampering.

### Worker Health Monitoring

Heartbeat mechanism detects failed workers automatically.

### Dynamic Load Redistribution

When a worker dies, traffic is redistributed among remaining workers.

### Real-Time Dashboard

Coordinator displays live metrics in the terminal.

### HTML Reporting

Automatically generates interactive reports with charts.

### Ramp-Up Support

Gradually increases traffic to avoid sudden spikes.

### Percentile Tracking

Measures:

- P50
- P95
- P99

latencies in real time.

---

# System Design

## Coordinator

Responsibilities:

- Worker registration
- Configuration distribution
- Test orchestration
- Metrics aggregation
- Failure handling
- Dashboard rendering
- Report generation

---

## Worker

Responsibilities:

- Load generation
- Metrics collection
- Heartbeat responses
- Final report submission

---

## Protocol Layer

Wire format:

```text
[4-byte payload length]
[64-byte HMAC signature]
[JSON payload]
```

Message Types:

```text
REGISTER
REGISTER_ACK
CONFIG
START
STOP
METRICS
REPORT
HEARTBEAT
HEARTBEAT_ACK
RAMP_UPDATE
ERROR
```

---

# Project Structure

```text
hydraload/
│
├── coordinator.py
├── worker.py
├── protocol.py
├── dummy_server.py
├── config.yaml
├── report.html
│
├── docs/
│   └── architecture.png
│
└── README.md
```

---

# Technologies Used

- Python 3
- TCP Sockets
- Multithreading
- HMAC-SHA256
- JSON Messaging
- Chart.js
- HTML/CSS
- Custom Networking Protocol

---

# Installation

Clone the repository:

```bash
git clone https://github.com/yourusername/hydraload.git

cd hydraload
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

# Generate Token

Generate a secure authentication token:

```bash
python coordinator.py --gen-token
```

Example:

```text
4f9c57d6f17d9a0b4b3e65e6a41f0a3b...
```

---

# Configure Token

## Windows CMD

```cmd
set LOAD_TEST_TOKEN=your_generated_token
```

## Windows PowerShell

```powershell
$env:LOAD_TEST_TOKEN="your_generated_token"
```

## Linux / macOS

```bash
export LOAD_TEST_TOKEN=your_generated_token
```

---

# Running a Demo

## Step 1 — Start Dummy Target Server

```bash
python dummy_server.py
```

Output:

```text
Dummy target running on http://127.0.0.1:8080
```

---

## Step 2 — Start Coordinator

```bash
python coordinator.py \
  --target http://127.0.0.1:8080 \
  --rps 100 \
  --duration 30 \
  --min-workers 2
```

Output:

```text
listening on 0.0.0.0:9500
waiting for 2 workers...
```

---

## Step 3 — Start Worker A

```bash
python worker.py --id worker-A
```

Output:

```text
registered as worker-A
```

---

## Step 4 — Start Worker B

```bash
python worker.py --id worker-B
```

Output:

```text
registered as worker-B
```

---

## Live Dashboard

Example:

```text
==============================================================
        DISTRIBUTED LOAD TEST — LIVE DASHBOARD
==============================================================

Target:    http://127.0.0.1:8080
Workers:   2 connected

WORKER           RPS     AVG     P50     P95     P99    ERR
------------------------------------------------------------
worker-A        42.1    89ms    84ms   150ms   166ms      2
worker-B        43.0    95ms    88ms   153ms   170ms      3
------------------------------------------------------------
TOTAL           85.1    92ms    88ms   152ms   170ms      5

████████████████████████████████████ 100%

Total Requests: 2,373
```

---

# Sample Results

| Metric | Value |
|----------|----------|
| Requests | 2,373 |
| Peak RPS | 88 |
| Avg Latency | 92 ms |
| P95 Latency | 152 ms |
| Error Rate | 4.17% |
| Workers | 2 |

---

# HTML Report

HydraLoad automatically generates:

```text
report.html
```

The report includes:

- Throughput Graph
- Average Latency
- P95 Latency
- P99 Latency
- Error Distribution
- Request Statistics
- Worker Summary

Open in any browser:

```bash
report.html
```

---

# Failure Recovery

HydraLoad supports worker failure handling.

If a worker disconnects:

1. Heartbeat timeout occurs
2. Coordinator marks worker as dead
3. Remaining workers receive updated RPS targets
4. Test continues automatically

Example:

```text
worker-B lost

redistributed 100 RPS across 1 worker
```

---

# Security

Every message is authenticated using:

```text
HMAC-SHA256
```

Benefits:

- Message integrity
- Tamper detection
- Trusted worker communication
- Session authentication

---

# Example Use Cases

- API Performance Testing
- Distributed Benchmarking
- Backend Stress Testing
- Research Experiments
- Network Systems Projects
- Performance Engineering Studies
- Fault-Tolerance Experiments

---

# Learning Outcomes

This project demonstrates concepts from:

### Distributed Systems

- Coordinator–Worker Architecture
- Fault Detection
- Load Redistribution

### Computer Networks

- TCP Socket Programming
- Custom Protocol Design
- Message Framing

### Security

- HMAC Authentication
- Secure Message Verification

### Operating Systems

- Multithreading
- Synchronization
- Resource Management

### Performance Engineering

- Throughput Measurement
- Latency Analysis
- Load Generation

---

# Future Enhancements

- Docker Deployment
- Kubernetes Support
- Web Dashboard
- HTTPS Load Testing
- Prometheus Integration
- Grafana Dashboards
- Distributed Tracing
- gRPC Transport
- Auto Scaling Workers
- Cloud Deployment Support

---

# Resume Impact

This project demonstrates practical experience in:

- Distributed Systems
- Network Programming
- Performance Engineering
- Fault Tolerance
- Security Engineering

and serves as a strong systems-oriented portfolio project for:

- Research Internships
- Systems Engineering Roles
- Backend Engineering Roles
- MS / PhD Applications

---

# License

MIT License

Copyright (c) 2026

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files to deal in the Software without restriction.
