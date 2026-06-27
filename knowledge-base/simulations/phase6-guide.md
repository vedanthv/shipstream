# Simulation Guide — Phase 6: Producer Delivery Guarantees

> **You are here:** [Index](../README.md) → **Phase 6 Simulation Guide**

These simulations target the three concepts from [Producer Delivery Guarantees](../kafka/producer-delivery-guarantees.md) that are easiest to misunderstand in theory and hardest to debug in production: ack levels, idempotence, and batching.

---

## Prerequisites

```bash
# All three brokers must be running
docker compose up -d

# Verify the cluster is healthy (all 3 nodes)
docker exec redpanda-1 rpk cluster health
```

S12 kills a broker container mid-produce. Make sure `docker compose up -d` is run again between simulations to restore the cluster to full health.

---

## S12 — acks=1 vs acks=all: the durability gap

**Concept:** [acks](../kafka/producer-delivery-guarantees.md#the-acks-spectrum)

**What it shows:** With `acks=1`, the producer receives a success ACK before replication completes. If the leader crashes in that window, confirmed messages vanish. With `acks=all`, the ACK only arrives after every ISR member has a copy — a leader crash cannot cause loss.

```bash
python3 simulations/sim_acks_comparison.py
```

**What to look for in the output:**

- The script finds the current leader for the simulation topic, then kills it mid-produce.
- Round 1 (`acks=1`): some messages that the producer reported as confirmed will be missing from the log after the new leader takes over. The gap is the durability hole.
- Round 2 (`acks=all`): confirmed count equals messages on disk. No gap.

```
Round 1 — acks=1
  Producer confirmed: 47 messages
  Messages on disk (new leader): 43 messages
  Gap: 4 messages — confirmed by producer, lost on leader crash

Round 2 — acks=all
  Producer confirmed: 51 messages
  Messages on disk: 51 messages
  Gap: 0 — no data loss
```

**Why the gap varies:** it depends on how quickly the leader crashes relative to when followers last fetched. A follower that just fetched will have more messages; one that hasn't fetched recently will have fewer.

**The fix is not retries.** With `acks=1`, the producer already received success and cleared the message from its buffer. It will not retry. The message is gone. `acks=all` is the only setting that prevents this.

---

## S13 — Idempotence: eliminating retry duplicates

**Concept:** [Idempotent producer](../kafka/producer-delivery-guarantees.md#idempotent-producer)

**What it shows:** Without idempotence, a message that times out and is retried may land twice — the original write succeeded at the broker but the ACK never reached the producer. With `enable.idempotence=true`, the broker deduplicates retries using the (producer ID, sequence number) pair.

```bash
python3 simulations/sim_idempotence.py
```

**What to look for in the output:**

- The script produces N messages and briefly pauses the broker mid-way to force timeouts and retries.
- Round 1 (no idempotence): total messages on disk > N. The duplicates are the retried messages that the broker had already written.
- Round 2 (idempotence on): total messages on disk = N. Retries are silently discarded at the broker.

```
Round 1 — enable.idempotence=False
  Messages sent (expected): 50
  Messages found on disk:   57
  Duplicates: 7

Round 2 — enable.idempotence=True
  Messages sent (expected): 50
  Messages found on disk:   50
  Duplicates: 0
```

**What makes this hard to catch in production:** the producer sees 50 successful deliveries both times. From its perspective, everything worked. The duplicates are invisible unless you read the log or your downstream consumer counts distinct message IDs.

---

## S14 — linger.ms: batching and throughput

**Concept:** [linger.ms](../kafka/producer.md#lingerms--the-coalescing-window), [compression.type](../kafka/producer-delivery-guarantees.md#compressiontype)

**What it shows:** `linger.ms=0` sends each message as soon as it is produced — one batch per message, maximum round trips. `linger.ms=50` holds messages for up to 50ms, coalescing them into larger batches — fewer round trips, higher throughput, slightly higher per-message latency.

```bash
python3 simulations/sim_linger_ms.py
```

**What to look for in the output:**

- Round 1 (`linger.ms=0`): many small batches, close to one per message. Total time is dominated by round-trip latency × number of messages.
- Round 2 (`linger.ms=50`): fewer, larger batches. Total time is lower for the same message count. Per-batch throughput is higher.

```
Round 1 — linger.ms=0
  Messages: 500
  Duration: 4.31s
  Throughput: 116 msg/s
  Avg delivery latency: 8.6ms

Round 2 — linger.ms=50
  Messages: 500
  Duration: 1.08s
  Throughput: 463 msg/s
  Avg delivery latency: 21.3ms
```

**The tradeoff:** per-message latency increases with `linger.ms` (messages wait before sending). Total throughput increases. For a real-time pipeline that must deliver each order with minimal delay, `linger.ms=0` or a small value is right. For a bulk import or analytics pipeline where throughput matters more than latency, `linger.ms=50` or higher is better.

**Combined with compression:** larger batches from `linger.ms` compress better, multiplying the throughput gain. The simulation optionally shows this with `compression.type=lz4`.

---

## Running all Phase 6 simulations

```bash
# S12 — acks durability gap (kills and restarts a broker, ~60 seconds)
python3 simulations/sim_acks_comparison.py

# Restore cluster before next sim
docker compose up -d
sleep 10

# S13 — idempotence deduplication (~45 seconds)
python3 simulations/sim_idempotence.py

# S14 — linger.ms batching (~30 seconds)
python3 simulations/sim_linger_ms.py
```

All output is printed to the terminal. S12 also logs to `logs/simulations/acks_comparison.log`.

---

> ← [Phase 5: Replication](./phase5-guide.md) | [Index](../README.md)
