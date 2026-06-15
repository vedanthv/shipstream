# Simulation Guide — Phase 4: Log Retention & Storage

> **You are here:** [Index](../README.md) → **Phase 4 Simulation Guide**

These simulations use lightweight topics created specifically for each run so they don't interfere with the main `order.created` pipeline. Each one targets a specific concept from [Log Retention & Storage Configuration](../kafka/log-retention.md).

---

## Prerequisites

```bash
# Infrastructure must be running
docker compose up -d

# Verify broker is healthy
docker exec redpanda rpk cluster health
```

---

## S6 — Active segment blocks retention

**Concept:** [log.segment.bytes](../kafka/log-retention.md#logsegmentbytes)

**What it shows:** Retention can only delete **closed** segments. If a low-volume topic never fills its segment, messages inside it will never be deleted — even if they are far past the retention window. This is the most common reason retention appears "broken" in production.

```bash
python3 simulations/sim_segment_roll.py
```

**What to look for in the log** (`logs/simulations/segment_roll.log`):

- Round 1: topic with 10 MB segments and 5-second retention. After 10 seconds, the low watermark is still 0. The active segment never rolled, so nothing was eligible for deletion.
- Round 2: same retention (5 seconds), but segments are 512 bytes. Messages force multiple rolls during produce. After 10 seconds, the low watermark has advanced — old closed segments were deleted on schedule.

```mermaid
flowchart TD
    subgraph R1["Round 1 — 10 MB segment, 5s retention"]
        P1["Produce 20 msgs\n~3.5 KB total"]
        A1["Active segment\n3.5 KB / 10 MB\nnever rolls"]
        W1["Wait 10 seconds\n(2× retention window)"]
        O1["Low watermark = 0\nNo deletion — active\nsegment still open"]
    end

    subgraph R2["Round 2 — 512 B segment, 5s retention"]
        P2["Produce 20 msgs\n~3.5 KB total"]
        A2["Segments roll every\n~3 messages\nmost are CLOSED"]
        W2["Wait 10 seconds\n(2× retention window)"]
        O2["Low watermark > 0\nClosed segments deleted\non schedule ✓"]
    end

    P1 --> A1 --> W1 --> O1
    P2 --> A2 --> W2 --> O2

    style O1 fill:#f8d7da,stroke:#dc3545
    style O2 fill:#d4edda,stroke:#28a745
```

**The fix for low-throughput topics:** Set `log.segment.ms` in addition to `log.segment.bytes`. This forces a roll on time, not just size — even if the segment never fills up.

```properties
log.segment.bytes=268435456   # 256 MiB
log.segment.ms=3600000        # force roll after 1 hour regardless of size
```

---

## S7 — log.retention.bytes: per-partition size cap

**Concept:** [log.retention.bytes](../kafka/log-retention.md#logretentionbytes)

**What it shows:** `log.retention.bytes` is a **per-partition** cap, not a per-topic cap. A 3-partition topic with `retention.bytes=5KB` can hold 15 KB total. When one partition overflows its cap, only that partition's oldest segments are deleted — the others are untouched.

```bash
python3 simulations/sim_retention_bytes.py
```

**What to look for in the log** (`logs/simulations/retention_bytes.log`):

- Round 1: 100 messages across 3 partitions (~6 KB per partition, over the 5 KB cap). After 15 seconds, all three partitions show advanced low watermarks — each was independently trimmed.
- Round 2: 30 messages routed to the same partition via a fixed key. Only that partition overflows its cap. Only that partition's low watermark advances; the others stay unchanged.

```mermaid
flowchart LR
    subgraph Topic["sim.retention-bytes — 3 partitions, retention.bytes = 5 KB each"]
        subgraph P0["Partition 0\n6 KB → trimmed to 5 KB"]
            S0A["seg-A (old)\nDELETED"]
            S0B["seg-B\nkept"]
        end
        subgraph P1["Partition 1\n6 KB → trimmed to 5 KB"]
            S1A["seg-A (old)\nDELETED"]
            S1B["seg-B\nkept"]
        end
        subgraph P2["Partition 2\n2 KB — under cap"]
            S2A["seg-A\nkept"]
        end
    end

    Cap["retention.bytes = 5 KB\nenforced independently\nper partition"]
    Cap --> P0
    Cap --> P1
    Cap --> P2

    style S0A fill:#f8d7da,stroke:#dc3545
    style S1A fill:#f8d7da,stroke:#dc3545
    style P2 fill:#d4edda,stroke:#28a745
```

**Capacity planning formula:**

```
total disk per topic  = partitions × retention.bytes
total disk per broker = Σ (partitions × retention.bytes) × replication_factor
```

---

## S8 — Log start offset and OFFSET_OUT_OF_RANGE

**Concept:** [The log start offset and what happens when data is gone](../kafka/log-retention.md#the-log-start-offset-and-what-happens-when-data-is-gone)

**What it shows:** When a consumer group's committed offset falls below the log start offset (because the segments containing those offsets were deleted), the next `poll()` returns an `OFFSET_OUT_OF_RANGE` error. The three `auto.offset.reset` policies handle this very differently — and choosing the wrong one leads to silent data loss.

```bash
python3 simulations/sim_log_start_offset.py
```

**What to look for in the log** (`logs/simulations/log_start_offset.log`):

- Step 1: 30 messages produced. Three consumer groups each consume and commit offset 10.
- Step 2: 12 seconds pass. Log start offset advances past offset 10. Committed offsets are now below the log start offset.
- Step 3: Three resume attempts, one per reset policy:
  - `earliest` — silently resumes from the new log start offset, skipping deleted messages with no error
  - `latest` — jumps to the tip of the log, skipping everything
  - `error` — raises an exception; consumer halts

```mermaid
flowchart TD
    Commit["Consumer group\ncommitted offset = 10"]
    Delete["Retention deletes\nsegments 0–15\nLog start offset → 16"]
    Poll["Next poll()"]
    OOR["OFFSET_OUT_OF_RANGE"]

    E["auto.offset.reset=earliest\n→ reset to offset 16\n→ skips 10–15 silently"]
    L["auto.offset.reset=latest\n→ reset to log end\n→ skips everything"]
    Err["auto.offset.reset=error\n→ raises exception\n→ consumer halts ✓"]

    Commit --> Delete --> Poll --> OOR
    OOR --> E
    OOR --> L
    OOR --> Err

    style E fill:#fff3cd,stroke:#ffc107
    style L fill:#f8d7da,stroke:#dc3545
    style Err fill:#d4edda,stroke:#28a745
```

**Production recommendation:** Use `auto.offset.reset=error` for any pipeline where missing messages is unacceptable (payments, orders, audit logs). Silent resets with `earliest` or `latest` mean silent data loss — the consumer moves on, lag disappears, and nothing alerts you that events were skipped.

**With tiered storage this problem goes away.** Old segments uploaded to S3 before local deletion mean the log start offset on local disk is no longer the true earliest offset. Consumers can still read offset 10 — the broker fetches it from S3 transparently.

---

## Running all Phase 4 simulations

```bash
# S6 — segment roll blocks retention (~30 seconds)
python3 simulations/sim_segment_roll.py

# S7 — per-partition retention.bytes cap (~40 seconds)
python3 simulations/sim_retention_bytes.py

# S8 — log start offset and OFFSET_OUT_OF_RANGE (~35 seconds)
python3 simulations/sim_log_start_offset.py
```

All logs land in `logs/simulations/`.

---

> ← [Phase 3: Broker Internals](./phase3-guide.md) | [Index](../README.md)
