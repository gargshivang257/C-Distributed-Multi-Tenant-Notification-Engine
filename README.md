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
                                      