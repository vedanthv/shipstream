# Producer Delivery Guarantees

> **Builds on:** [Chapter 5 — Producer](./producer.md) which introduced `acks`, `linger.ms`, and `flush()` at a surface level. This chapter goes into the mechanics and covers the gaps.

---

## The acks spectrum

`acks` is the single most important producer config. It controls how many brokers must confirm a write before the producer considers it delivered.

### acks=0 — fire and forget

The producer sends the message and moves on immediately. No acknowledgment is waited for. The broker may or may not have written it.

```
Producer          Broker
   │──── produce ──▶│
   │                │  (writes to disk... maybe)
   │  (no response waited for)
```

**What you get:** maximum throughput — no round trips, no blocking.  
**What you lose:** any write that hits a slow broker, a full buffer, or a crash is silently gone. The producer has no way to know.

Use it for metrics, logs, or any pipeline where occasional loss is acceptable and throughput is paramount.

### acks=1 — leader confirms

The leader writes the message to its local log and sends an ACK immediately. Followers replicate asynchronously — the ACK does not wait for them.

```
Producer          Leader (Broker 1)      Follower (Broker 2)
   │──── produce ────▶│                        │
   │                  │ writes to disk         │
   │◀──── ACK ────────│                        │
   │                  │── replicates ─────────▶│  (async, after ACK)
```

**The gap:** between the ACK and the follower catching up, the leader is the only copy. If the leader crashes in that window:

```
Producer          Leader (Broker 1)      Follower (Broker 2)
   │──── produce ────▶│                        │
   │◀──── ACK ────────│                        │
   │                  │  💥 crash              │ ← only at offset N-1
   │                  ✗               elected as leader at offset N-1
   │  (confirmed msg                           │
   │   is gone —                               │
   │   producer won't retry)
```

The producer already discarded the message from its internal buffer after receiving the ACK. The new leader never had it. Silent data loss.

### acks=all (or -1) — all ISR members confirm

The leader waits for every member of the ISR to fetch and acknowledge before sending the ACK to the producer.

```
Producer          Leader (Broker 1)      Follower (Broker 2)   Follower (Broker 3)
   │──── produce ────▶│                        │                      │
   │                  │── replicates ─────────▶│                      │
   │                  │── replicates ──────────────────────────────▶  │
   │                  │◀── ack ────────────────│                      │
   │                  │◀── ack ─────────────────────────────────────  │
   │◀──── ACK ────────│
```

By the time the producer gets success, every ISR broker has a copy. A leader crash cannot cause data loss — the new leader (elected from ISR) already has the message.

**The cost:** latency. The leader must wait for the slowest ISR member before responding. In a busy cluster, this is milliseconds. In a degraded cluster with a lagging follower, it can be much more.

### Choosing

| acks | Survives client crash | Survives leader crash | Latency |
|---|---|---|---|
| 0 | No | No | Lowest |
| 1 | Yes | No | Low |
| all | Yes | Yes | Higher |

For `order.created` — financial pipeline, confirmed writes must be durable — use `acks=all`.

---

## Idempotent producer

### The retry duplicate problem

With `acks=all` and `retries > 0`, the producer retries on timeout or transient failure. The problem: the original request may have succeeded at the broker before the timeout — the network just dropped the ACK on the way back. The retry then writes the message a second time, creating a duplicate.

```
Producer                        Broker
   │──── produce(msg A) ──────────▶│ writes msg A at offset 100
   │                                │
   │     (ACK lost in transit)      │
   │                                │
   │  timeout — retry               │
   │──── produce(msg A) ──────────▶│ writes msg A at offset 101  ← DUPLICATE
   │◀──── ACK ──────────────────────│
```

The producer sees one successful delivery. The log has two copies.

### enable.idempotence=true

```python
producer = Producer({
    "bootstrap.servers": BROKER,
    "enable.idempotence": True,   # implies acks=all, retries=INT_MAX
})
```

When idempotence is enabled:

1. The broker assigns the producer a **producer ID (PID)** on first connect.
2. The producer attaches a **monotonic sequence number** to every message, per partition.
3. The broker tracks the last sequence number it committed for each (PID, partition) pair.
4. On a retry, the broker sees the same (PID, sequence) and discards the duplicate — without returning an error.

```
Producer (PID=42)               Broker
   │──── produce(seq=0, msg A) ──▶│ writes msg A at offset 100, records seq=0
   │                               │
   │     (ACK lost)                │
   │                               │
   │──── produce(seq=0, msg A) ──▶│ seq=0 already committed → discard silently
   │◀──── ACK ─────────────────────│
```

The log has one copy. The producer sees one delivery. Exactly-once at the producer level.

`enable.idempotence=true` also forces `acks=all` and sets `retries` to its maximum. You cannot have idempotence with `acks=1` — it wouldn't make sense (the broker can't deduplicate if it never told you it succeeded).

---

## max.in.flight.requests.per.connection

This controls how many produce requests can be outstanding (sent but not yet acknowledged) on a single connection at once.

### Without idempotence

Assume `max.in.flight.requests.per.connection=5` and retries are enabled:

```
Producer sends: batch1(seq 0-9), batch2(seq 10-19), batch3(seq 20-29)
                 all in flight simultaneously

batch2 fails → retried
batch3 succeeds (arrives before the retry of batch2)
batch2 retry succeeds

Log: seq 0-9, seq 20-29, seq 10-19  ← reordered
```

With `>1` in-flight requests and retries, message ordering can break. The fix without idempotence: set `max.in.flight.requests.per.connection=1`. One request at a time. Ordering guaranteed but throughput limited.

### With idempotence

The broker can detect and reject out-of-order sequences using the (PID, sequence) pair. With `enable.idempotence=true`, up to 5 in-flight requests are safe:

```python
"enable.idempotence": True,
"max.in.flight.requests.per.connection": 5,  # safe with idempotence
```

The broker reorders or rejects duplicates at the log level. You get both ordering and parallelism.

---

## delivery.timeout.ms

`delivery.timeout.ms` is the **total time budget** for a produce call — from the first attempt through all retries through the final acknowledgment.

```
produce() called
│
├── linger.ms window (coalescing)
├── first attempt → timeout → retry 1 → timeout → retry 2 → ...
│
└── delivery.timeout.ms expires → BufferError / callback(err)
```

It acts as an outer bound on everything: linger time, queue time, network time, retry delays. Once it expires, the message is dropped from the buffer and the delivery callback is called with an error.

The relationship with `retries`:

- `retries=INT_MAX` + `delivery.timeout.ms=120000` (2 minutes, the default): the producer retries indefinitely for up to 2 minutes. If the broker recovers within that window, the message lands. If not, it fails.
- `retries=0` + any `delivery.timeout.ms`: one attempt, no retries.

For a pipeline that must survive short broker restarts (under 30 seconds), a `delivery.timeout.ms` of 60–120 seconds with `enable.idempotence=true` is the right combination.

---

## compression.type

The producer can compress batches before sending them, reducing network payload and broker disk usage.

```python
"compression.type": "lz4",   # none | gzip | snappy | lz4 | zstd
```

Compression is applied per **batch**, not per message. Larger batches compress better — this is why `linger.ms` and compression are tuned together.

| codec | CPU cost | Ratio | Notes |
|---|---|---|---|
| none | zero | 1.0× | baseline |
| gzip | high | best | good for archival topics with low throughput |
| snappy | low | moderate | Google's format; balanced |
| lz4 | very low | good | default production choice; fast enough to be CPU-neutral |
| zstd | moderate | best-in-class | newer; better ratio than gzip at lower CPU than gzip |

**The broker decompresses for consumers.** The consumer-side client receives the raw messages, not compressed bytes — this is handled transparently.

**One subtlety:** the broker also validates message content during compression, so setting compression at the producer does not bypass schema validation at the registry.

For ShipStream's Protobuf orders: Protobuf binary is already compact. `lz4` is the right default — it adds minimal CPU overhead and gives ~20–30% size reduction on typical Protobuf payloads.

---

## buffer.memory and max.block.ms

The producer maintains an internal buffer of unsent messages. `produce()` writes to this buffer; the I/O thread drains it.

```
application thread          I/O thread
      │                          │
  produce(msg)                   │
      │                          │
  write to buffer ──────────────▶│── send to broker
      │                          │◀── ACK
      │                          │── fire delivery callback
```

`buffer.memory` (default: 32 MB) is the total size of this buffer. If the broker is slow or unavailable, the buffer fills up.

When the buffer is full, `produce()` does not fail immediately — it **blocks** for up to `max.block.ms` (default: 60 seconds), waiting for space to free up. If no space opens up in time, it raises a `BufferError`.

```python
try:
    producer.produce(topic, value=payload, callback=on_delivery)
except BufferError:
    # buffer is full — broker is not keeping up
    # options: drop the message, log and alert, apply backpressure upstream
    pass
```

**What causes buffer exhaustion:**
- Broker is down or slow to ack
- `delivery.timeout.ms` is very long (messages accumulate waiting for retry)
- Produce rate exceeds the broker's write throughput

**Tuning:**
- Raise `buffer.memory` to absorb larger bursts
- Lower `max.block.ms` to fail fast rather than block the application thread for 60 seconds
- For pipelines where dropping is unacceptable, catch `BufferError` and apply upstream backpressure

---

## The full producer config for production

```python
producer = Producer({
    "bootstrap.servers": BROKER,

    # Durability
    "enable.idempotence": True,        # implies acks=all, max retries
    "delivery.timeout.ms": 120_000,    # 2-minute outer budget

    # Batching
    "linger.ms": 5,                    # coalesce for 5ms
    "batch.size": 65_536,              # 64KB batch cap
    "compression.type": "lz4",         # low-CPU compression

    # Parallelism (safe with idempotence)
    "max.in.flight.requests.per.connection": 5,
})
```

This gives you: durability (no loss on broker crash), deduplication (no loss on retry), efficient batching, and safe parallelism.

---

## Next

[Phase 6 Simulations: S12, S13, S14 →](../simulations/phase6-guide.md)