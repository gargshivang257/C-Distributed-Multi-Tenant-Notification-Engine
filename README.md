# C++ Distributed Multi-Tenant Notification Engine

A high-performance, low-latency notification gateway and concurrent thread scheduler built from scratch in modern **ISO C++20**. This engine optimized multi-tenant message distribution by stripping out traditional kernel-level synchronization locks and replacing them with atomic, lock-free work-stealing collections operating directly across CPU cache lines.

---

## Technical Architecture Overview

The system decouples high-frequency network ingestion from asynchronous execution tiers through a multi-stage pipeline:

```text
       [ HIGH-FREQUENCY TRAFFIC INGESTION ]
                        │
                        ▼
       ┌─────────────────────────────────┐
       │     Idempotency Engine          │ <- SHA-256 Memory-Mapped Duplication Filter
       └─────────────────────────────────┘
                        │
                        ▼
       ┌─────────────────────────────────┐
       │     Sharded Rate Limiter        │ <- Token-Bucket Multi-Tenant Boundary
       └─────────────────────────────────┘
                        │
                        ▼
          [ LOCK-FREE DEQUE ROUTER ]
          Distributed Tasks Placement
         ┌──────────────┴──────────────┐
         ▼                             ▼
┌──────────────────┐          ┌──────────────────┐
│  Worker Array 0  │          │  Worker Array 1  │ <- In-Memory Cache Lines (Fixed Cap 1024)
└────────┬─────────┘          └────────┬─────────┘
         │                             │   ▲
         ▼ (Local PopBack)             │   │ (Lock-Free StealFront CAS)
  [CRITICAL EXECUTION]                 └───┼──────────────────┐
                                           │                  │
                                   ┌───────┴──────────┐┌──────┴───────────┐
                                   │ Dynamic Worker 7 ││ Dynamic Worker 8 │
                                   └──────────────────┘└──────────────────┘
                                      [ ATOMIC MATHEMATICAL STEALING POOL ]
                                      
```

Idempotency Layer: Evaluates incoming notification payloads using a non-blocking SHA-256 memory-mapped bit-shifting hashing mechanism to isolate historical tracking keys and seamlessly reject duplicate requests.

Sharded Rate Limiter: Enforces multi-tenant throughput boundaries via an optimized, thread-isolated token-bucket configuration that prevents cross-tenant performance degradation without relying on heavy global locks.

Lock-Free Concurrency Array: Allocates discrete memory slices to execution nodes via single-producer multi-consumer ring buffers.

Mathematical Scheduling Framework: Idle dynamic threads monitor sibling workloads and apply exponential starvation formulas to execute cross-thread task steals via atomic primitives.

---

## Core Engineering Deep-Dives

1. Lock-Free Dual-Index Circular Ring Buffers

To minimize thread dispatch latency, traditional mutex locks were replaced with a custom single-producer multi-consumer queue governed by atomic head and tail pointers:

Cache Locality Optimization: The critical worker node that owns the queue invokes PopBack() locally using relaxed memory ordering (std::memory_order_relaxed) to access its own tail.

CAS Collision Prevention: External dynamic stealer threads target the opposite end via StealFront(). Memory sequencing is managed using explicit sequential consistency (std::memory_order_seq_cst) to protect index modification through a Compare-And-Swap (CAS) loop:

head_.compare_exchange_strong(h, h + 1, std::memory_order_seq_cst, std::memory_order_relaxed)

2. Mathematical Priority Aging Scheduler

To eliminate low-priority message starvation during periods of sustained heavy traffic, the core engine runtime evaluates backlogged tasks using an exponential decay model:

$$P_{\text{dynamic}} = P_{\text{base}} + \alpha \cdot (1 - e^{-\lambda \cdot \Delta t})$$

$P_{\text{base}} = 1.0$ (Baseline notification importance assignment).
$\alpha = 10.0$ (Priority acceleration threshold ceiling metric).
$\lambda = 0.005$ (Geometric age curve decay coefficient).

When a standard task waits in memory, its priority score dynamically scales upward. Once $P_{\text{dynamic}} > 1.05$, dynamic threads break their condition variable sleep states, short-circuit network boundaries, and execute a lock-free steal block straight from the busy thread's ring buffer.

3. Asynchronous Prometheus Telemetry Exporter

System telemetry (ingestion rates, latencies, and error codes) is updated without performance degradation using atomic arrays running relaxed operations (std::memory_order_relaxed).

Observability metrics are exposed through a custom network socket layer built entirely from scratch using native system sockets. Listening on port 8080, the background daemon outputs metrics conforming to the standard OpenTelemetry plaintext exposition format:

# HELP notification_ingestion_rate_total Cumulative count of notification events processed.
# TYPE notification_ingestion_rate_total counter
notification_ingestion_rate_total{tenant="tenant-alpha",status="200"} 1
notification_ingestion_rate_total{tenant="tenant-alpha",status="409"} 37
notification_ingestion_rate_total{tenant="tenant-alpha",status="429"} 2

---

## Project Structure

multi-tenant-notification-engine/
├── src/
│   ├── agent/       # Functional Context Routing Rules
│   ├── gateway/     # Rate Limiter, Deduplication, Telemetry HTTP Server
│   ├── queue/       # AMQP Client Simulation Interface
│   ├── worker/      # Lock-Free Ring Buffer, Work-Stealing Pool Logic
│   └── main.cpp     # Orchestration Runtime Entry Point
├── CMakeLists.txt   # Unified build configuration system
├── CMakePresets.json# Unified compilation preset parameters
└── README.md        # Architecture Whitepaper Documentation

---

## Build and Execution Guidelines

Pre-requisites
CMake Version $\ge 3.20$
MSVC v143 or GCC Compiler Toolchain supporting C++20 standard
Local Redis instance configured on default port 637
9OpenSSL Win64 toolkit dependencies

---

## Compilation Pipeline

From your project directory root, execute the specific build orchestration configuration:

:: 1. Clear out previous build tracks if necessary
rmdir /s /q build

:: 2. Configure the project toolchain paths natively
cmake -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DOPENSSL_ROOT_DIR="C:/Program Files/OpenSSL-Win64" -DCMAKE_TOOLCHAIN_FILE="C:/Users/Shivang Garg/Desktop/vcpkg/scripts/buildsystems/vcpkg.cmake"

:: 3. Build the optimized Release target configuration
cmake --build build --config Release

---

## Run the Application Runtime

Make sure your local Redis instance is running (redis-server), then launch the engine:

build\Release\notification_engine.exe

---

## Scraping the Telemetry Port

While the simulator loop runs, verify the OpenTelemetry pipeline via terminal curl or a standard web browser:

curl http://localhost:8080/metrics