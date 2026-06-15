# Kafka & Redpanda Threading Models II

> A deep-dive into how Kafka handles disk I/O, thread coordination, and how Redpanda's thread-per-core model eliminates the bottlenecks.

---

## Table of Contents

- [How Kafka Writes to Disk](#how-kafka-writes-to-disk)
- [Kafka Thread Architecture](#kafka-thread-architecture)
- [What Blocks an I/O Thread](#what-blocks-an-io-thread)
- [fsync: What "Blocked Until Durable" Means](#fsync-what-blocked-until-durable-means)
- [Do I/O Threads Own Fixed Partitions?](#do-io-threads-own-fixed-partitions)
- [Lock Contention & fsync Cascade](#lock-contention--fsync-cascade)
- [Durability vs Throughput Trade-off](#durability-vs-throughput-trade-off)
- [How Redpanda Solves This](#how-redpanda-solves-this-thread-per-core-model)

---

## How Kafka Writes to Disk

Kafka does **not** write directly to disk. It uses the **OS page cache** as an intermediary:

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'secondaryColor': '#0f2540', 'tertiaryColor': '#162032', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'clusterBkg': '#0f2540', 'titleColor': '#e2e8f0', 'edgeLabelBackground': '#0d1117', 'activeTaskBkgColor': '#4a9eff'}}}%%
sequenceDiagram
    participant P as 🟢 Producer
    participant NT as Network Thread
    participant PC as OS Page Cache
    participant D as 💾 Disk

    P->>NT: Send message
    NT->>PC: Write to page cache (nanoseconds ⚡)
    NT-->>P: Acknowledge (if acks=1)
    Note over PC,D: OS flushes asynchronously in background
    PC-->>D: Lazy flush (background)
```

**Key insight**: The I/O thread only blocks for a **memory write** (nanoseconds), not an actual disk write — unless `fsync` is explicitly called.

---

## Kafka Thread Architecture

Kafka uses three separate thread pools. They operate independently — a slow I/O thread does **not** block a network thread.

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'secondaryColor': '#0f2540', 'tertiaryColor': '#162032', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'clusterBkg': '#0f2540', 'titleColor': '#e2e8f0'}}}%%
flowchart TD
    P1["🟢 Producer 1"] & P2["🟢 Producer 2"] & P3["🟢 Producer 3"] --> NL

    subgraph NL["Network Layer (num.network.threads)"]
        NT1["Network Thread 1"]
        NT2["Network Thread 2"]
        NT3["Network Thread 3"]
    end

    NL --> RQ["📥 Shared Request Queue"]

    RQ --> IL

    subgraph IL["I/O Layer (num.io.threads)"]
        IT1["I/O Thread 1"]
        IT2["I/O Thread 2"]
        IT3["I/O Thread 3"]
        IT4["I/O Thread 4"]
    end

    IL --> PC["🗂 OS Page Cache"]
    PC -.->|async flush| D["💾 Disk"]

    style NL fill:#0f2540,stroke:#4a9eff,color:#e2e8f0
    style IL fill:#0f2540,stroke:#4a9eff,color:#e2e8f0
    style RQ fill:#1e3a5f,stroke:#4a9eff,color:#e2e8f0
    style PC fill:#162032,stroke:#4a9eff,color:#e2e8f0
    style D fill:#162032,stroke:#4a9eff,color:#e2e8f0
```

| Thread Pool | Config | Role |
|---|---|---|
| Network threads | `num.network.threads` | Handle socket connections, read/write from network |
| I/O threads | `num.io.threads` | Read/write to log segments on disk |
| Request handler threads | `num.io.threads` (shared) | Process produce/fetch/metadata requests |

---

## What Blocks an I/O Thread

| Scenario | Blocks? | Why |
|---|---|---|
| Write to OS page cache | ✅ No — near instant | Memory operation |
| `fsync` to disk | ❌ **Yes** — fully blocked | Waits for OS + disk confirmation |
| Cold read (not in page cache) | ❌ **Yes** — blocked on disk seek | Physical I/O required |
| Log segment rolling | ⚠️ Briefly | File system operation |

---

## fsync: What "Blocked Until Durable" Means

Think of it like saving a Word document:
- **Page cache write** = typing changes into RAM (instant, lost if power dies)
- **fsync** = clicking "Save" and waiting for the disk indicator — the thread is **frozen** until done

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'secondaryColor': '#0f2540', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'titleColor': '#e2e8f0', 'edgeLabelBackground': '#0d1117', 'activeTaskBorderColor': '#4a9eff', 'activeTaskBkgColor': '#1e3a5f', 'taskBkgColor': '#0f2540', 'taskBorderColor': '#4a9eff', 'taskTextColor': '#e2e8f0', 'taskTextLightColor': '#e2e8f0', 'critBkgColor': '#3d1a1a', 'critBorderColor': '#ff6b6b', 'sectionBkgColor': '#0d1117', 'altSectionBkgColor': '#0f2540', 'gridColor': '#2d3748', 'doneTaskBkgColor': '#1a3a1a', 'doneTaskBorderColor': '#4ade80'}}}%%
gantt
    title I/O Thread 3 — Timeline during fsync (flush.messages=1)
    dateFormat x
    axisFormat %Lms

    section Thread State
    Write msg-1 to page cache     :done,    t1, 0, 1
    fsync() called — BLOCKED      :crit,    t2, 1, 6
    Disk confirms — Thread free   :done,    t3, 6, 7
    Write msg-2 to page cache     :done,    t4, 7, 8
    fsync() called — BLOCKED      :crit,    t5, 8, 13
    Disk confirms — Thread free   :done,    t6, 13, 14
```

**Impact on throughput**: If disk latency is ~5ms and you fsync every message:
- Max throughput = **200 messages/sec** (1000ms ÷ 5ms)
- Without fsync: **hundreds of thousands/sec** (page cache = nanoseconds)

### `flush.messages=1` vs default

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'clusterBkg': '#0f2540', 'titleColor': '#e2e8f0'}}}%%
flowchart LR
    subgraph BAD["flush.messages=1  ❌ Slow"]
        direction TB
        A1["msg-1 → page cache"] --> B1["fsync ⏳ 5ms BLOCKED"]
        B1 --> C1["msg-2 → page cache"] --> D1["fsync ⏳ 5ms BLOCKED"]
        D1 --> E1["msg-3 waits..."]
    end

    subgraph GOOD["flush.messages=default  ✅ Fast"]
        direction TB
        A2["msg-1 → page cache ⚡"] --> B2["msg-2 → page cache ⚡"]
        B2 --> C2["msg-3 → page cache ⚡"]
        C2 --> D2["OS flushes async 🔄"]
    end

    style BAD fill:#2d1515,stroke:#ff6b6b,color:#e2e8f0
    style GOOD fill:#0f2a1a,stroke:#4ade80,color:#e2e8f0
```

---

## Do I/O Threads Own Fixed Partitions?

**No.** Kafka uses a **shared thread pool** — any thread can handle any partition's request:

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'clusterBkg': '#0f2540', 'titleColor': '#e2e8f0'}}}%%
flowchart TD
    RQ["📥 Request Queue\nWrite P-3 · Write P-1 · Write P-7 · Write P-3 · Write P-2"]

    RQ --> T1["Thread 1\n→ picks Write P-3"]
    RQ --> T2["Thread 2\n→ picks Write P-1"]
    RQ --> T3["Thread 3\n→ picks Write P-7"]

    T1 --> L3["🔒 Acquire lock: P-3 log"]
    T2 --> L1["🔒 Acquire lock: P-1 log"]
    T3 --> L7["🔒 Acquire lock: P-7 log"]

    style RQ fill:#1e3a5f,stroke:#4a9eff,color:#e2e8f0
    style L3 fill:#0f2540,stroke:#4a9eff,color:#e2e8f0
    style L1 fill:#0f2540,stroke:#4a9eff,color:#e2e8f0
    style L7 fill:#0f2540,stroke:#4a9eff,color:#e2e8f0
```

What actually **serializes per partition** is the **log file lock** — not the thread assignment.

---

## Lock Contention & fsync Cascade

When fsync blocks a thread, other threads trying to write to the same partition pile up on the lock:

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'clusterBkg': '#0f2540', 'titleColor': '#e2e8f0', 'edgeLabelBackground': '#0d1117'}}}%%
flowchart TD
    T2["Thread 2\nholds P-3 lock"]
    T2 -->|calls| FS["fsync ⏳\nBLOCKED on disk ~5ms"]

    T4["Thread 4\npicks up Write P-3"]
    T4 -->|tries to acquire| LK["🔒 P-3 log lock"]
    LK -->|held by Thread 2| W["Thread 4 BLOCKED\nwaiting for lock"]

    T1["Thread 1 — P-1 ✅ free"]
    T3["Thread 3 — P-7 ✅ free"]

    style FS fill:#2d1515,stroke:#ff6b6b,color:#e2e8f0
    style W fill:#2d1515,stroke:#ff6b6b,color:#e2e8f0
    style T1 fill:#0f2a1a,stroke:#4ade80,color:#e2e8f0
    style T3 fill:#0f2a1a,stroke:#4ade80,color:#e2e8f0
    style LK fill:#2d2200,stroke:#fbbf24,color:#e2e8f0
```

**Result**: One slow fsync can tie up **two threads** — one blocked on disk, one blocked on the lock — reducing effective thread pool capacity.

---

## Durability vs Throughput Trade-off

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'background': '#0d1117', 'quadrant1Fill': '#0f2a1a', 'quadrant2Fill': '#1e3a5f', 'quadrant3Fill': '#2d1515', 'quadrant4Fill': '#2d2200', 'quadrantPointFill': '#4a9eff', 'quadrantPointTextFill': '#e2e8f0', 'quadrantXAxisTextFill': '#94a3b8', 'quadrantYAxisTextFill': '#94a3b8', 'quadrantInternalBorderStrokeFill': '#2d3748', 'quadrantExternalBorderStrokeFill': '#4a9eff', 'quadrantTitleFill': '#e2e8f0'}}}%%
quadrantChart
    title Throughput vs Durability
    x-axis Low Durability --> High Durability
    y-axis Low Throughput --> High Throughput
    quadrant-1 Best of both worlds
    quadrant-2 Fast but risky
    quadrant-3 Avoid
    quadrant-4 Safe but slow
    flush.messages=default + acks=1: [0.25, 0.90]
    acks=all + replication: [0.75, 0.65]
    flush.messages=1 + acks=all: [0.90, 0.15]
    Redpanda io_uring: [0.80, 0.90]
```

| Config | Throughput | Durability | Notes |
|---|---|---|---|
| `flush.messages=1` | 🔴 Very low | 🟢 Strongest | Survives power loss without replication |
| `flush.messages=default` | 🟢 Very high | 🟡 Weaker | Relies on OS + replication |
| `acks=all` + replicas | 🟡 Medium | 🟢 Strong | Recommended Kafka approach |
| Redpanda (`io_uring`) | 🟢 Very high | 🟢 Strong | Async fsync, no thread blocking |

> **Kafka's recommended approach**: skip frequent fsyncs, use `acks=all` + `min.insync.replicas=2`. If 3 brokers hold data in page cache, a single machine's power loss doesn't matter.

---

## How Redpanda Solves This: Thread-per-Core Model

Redpanda uses the **Seastar framework** where each CPU core **permanently owns** a fixed set of partitions.

### Architecture Comparison

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'clusterBkg': '#0f2540', 'titleColor': '#e2e8f0'}}}%%
flowchart LR
    subgraph KAFKA["☕ Kafka — Shared Thread Pool"]
        direction TB
        KQ["📥 Shared Queue\nP3 P1 P7 P3 P2"]
        KQ --> KT1["Thread 1"]
        KQ --> KT2["Thread 2"]
        KQ --> KT3["Thread 3"]
        KT1 & KT2 & KT3 -->|contend on| KL["🔒 Partition Locks"]
    end

    subgraph REDPANDA["🐼 Redpanda — Thread-per-Core"]
        direction TB
        RC0["Core 0\nOwns P-0, P-3, P-6\n(forever)"]
        RC1["Core 1\nOwns P-1, P-4, P-7\n(forever)"]
        RC2["Core 2\nOwns P-2, P-5, P-8\n(forever)"]
    end

    style KAFKA fill:#2d1515,stroke:#ff6b6b,color:#e2e8f0
    style REDPANDA fill:#0f2a1a,stroke:#4ade80,color:#e2e8f0
    style KL fill:#3d2200,stroke:#fbbf24,color:#e2e8f0
```

### Redpanda's Async Event Loop (io_uring)

Each core runs a **single-threaded async event loop**. Even fsync doesn't block:

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'secondaryColor': '#0f2540', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'titleColor': '#e2e8f0', 'edgeLabelBackground': '#0d1117'}}}%%
sequenceDiagram
    participant EL as Core-0 Event Loop
    participant IOU as io_uring (kernel)
    participant D as 💾 Disk

    EL->>EL: Write P-3 to page cache ⚡
    EL->>EL: Write P-6 to page cache ⚡
    EL->>IOU: Submit fsync (async) — no blocking!
    EL->>EL: Write P-0 to page cache ⚡ (keeps working!)
    EL->>EL: Write P-3 next message ⚡
    IOU-->>D: Flush to disk
    D-->>IOU: Confirm ✅
    IOU-->>EL: Completion callback fires
    EL->>EL: ACK producer ✅
```

### Cross-Core Communication

When a request arrives at the wrong core, Redpanda uses **lock-free message passing**:

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'clusterBkg': '#0f2540', 'titleColor': '#e2e8f0'}}}%%
sequenceDiagram
    participant C as Client
    participant C1 as Core-1
    participant C0 as Core-0 (owns P-3)

    C->>C1: Write request for P-3
    Note over C1: P-3 not mine!
    C1->>C0: Message via lock-free queue
    C0->>C0: Handle write for P-3
    C0-->>C1: Response
    C1-->>C: ACK ✅
    Note over C1,C0: No shared memory. No locks. No contention.
```

### What Redpanda Eliminates

| Problem in Kafka | Root Cause | Redpanda Fix |
|---|---|---|
| Lock contention on partition log | Multiple threads share one partition | One core owns each partition — no lock needed |
| fsync blocking threads | Blocking syscall freezes thread | `io_uring` submits async, core keeps working |
| fsync cascade (lock + disk wait) | Thread holds lock during fsync | No locks to hold |
| Thread pool exhaustion | Hot partitions monopolize threads | Each core is isolated — hot partition stays on its core |
| Context switching overhead | OS schedules many threads | Each core runs a tight loop, no preemption |

### Net Result

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'primaryColor': '#1e3a5f', 'primaryTextColor': '#e2e8f0', 'primaryBorderColor': '#4a9eff', 'lineColor': '#4a9eff', 'background': '#0d1117', 'mainBkg': '#1e3a5f', 'nodeBorder': '#4a9eff', 'clusterBkg': '#0f2540', 'titleColor': '#e2e8f0', 'xyChart': {'backgroundColor': '#0d1117', 'plotColorPalette': '#4a9eff,#4ade80'}}}}%%
xychart-beta
    title "Latency as Partition Count Increases"
    x-axis ["10", "50", "100", "500", "1000"]
    y-axis "p99 Latency (ms)" 0 --> 100
    line "Kafka" [5, 15, 30, 65, 95]
    line "Redpanda" [4, 5, 6, 7, 8]
```

- **Kafka**: more partitions → more lock contention → latency grows
- **Redpanda**: more partitions → evenly spread across cores → latency stays flat

---
