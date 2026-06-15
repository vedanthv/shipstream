# Chapter 3 — Offsets

> **You are here:** [Index](../README.md) → [Topics & Partitions](./topics-and-partitions.md) → **Offsets**

---

## What is an offset?

An offset is a **sequential integer that identifies a message's position within a partition**. It starts at 0 and increments by 1 for every new message.

Think of a partition as a book and the offset as the page number. The broker writes new pages at the end. Consumers bookmark their current page.

```
Partition 0
──────────────────────────────────────────────────────────
offset:   0     1     2     3     4     5     6     (7)
message: [A]   [B]   [C]   [D]   [E]   [F]   [G]    ← next write
```

---

## The full address of a message

Offsets are **per partition** — offset 5 means nothing without also knowing the topic and partition.

```
topic  +  partition  +  offset
─────────────────────────────────────────────
"order.created"  /  partition-2  /  offset-15
```

This triplet uniquely identifies any message in the system. You'll see it everywhere — in logs, in the Redpanda Console, in delivery callbacks.

---

## Three numbers to know

### Log End Offset (LEO)
Set by the **broker**. The offset of the next message to be written — i.e., the current length of the partition log. It increases every time a producer writes.

### Group Offset (Current Offset)
Set by the **consumer**. The offset up to which this consumer group has read and committed. Kafka stores this server-side so it survives restarts.

### Lag
`Lag = Log End Offset - Group Offset`

How many messages the group hasn't consumed yet. A lag of 0 means fully caught up. A growing lag means the consumer can't keep up with the producer.

```
Partition 0
──────────────────────────────────────────────────────────────
offset:   0     1     2     3     4     5     6     7     (8)
         [A]   [B]   [C]   [D]   [E]   [F]   [G]   [H]
                                   ↑                   ↑
                          Group Offset = 4    Log End Offset = 8

Lag = 8 - 4 = 4 messages behind
```

---

## Why offsets enable powerful patterns

### Pattern 1 — Replay
Because messages are never deleted on read, a consumer can reset its offset back to 0 and replay the entire history.

| Industry | Replay use case |
|----------|----------------|
| **Finance** | Replay all payment events to rebuild account balances after a bug corrupted the database |
| **E-commerce** | Replay all order events to populate a new analytics data warehouse |
| **Healthcare** | Replay patient vitals to re-run a new anomaly detection algorithm against historical data |
| **Gaming** | Replay player actions to reconstruct a game session for debugging a reported cheat |

### Pattern 2 — Multiple independent readers
Each consumer group has its own offset pointer on each partition. They never interfere.

```
Partition 0 (Log End Offset = 200)
─────────────────────────────────────────
inventory-service-group   → offset 200  (fully caught up, lag 0)
shipping-service-group    → offset 150  (50 messages behind)
analytics-group           → offset 50   (150 messages behind, batch processing)
```

All three read the same events. The analytics group being slow doesn't affect inventory or shipping.

### Pattern 3 — Exactly-once processing
By controlling when the group offset is committed (before or after processing), you can tune delivery semantics:

| Mode | How | Risk |
|------|-----|------|
| At-most-once | Commit before processing | Message lost if consumer crashes mid-process |
| At-least-once | Commit after processing | Message reprocessed if consumer crashes post-process pre-commit |
| Exactly-once | Transactional commit | Complex but no duplicates or losses |

---

## What the Log End Offset actually represents

The LEO is the broker's *claimed* position — it reflects messages that have been accepted into the log. But "in the log" does not mean "safely on disk."

By default, Kafka writes to the **OS page cache** first and lets the OS flush to disk in the background. The offset advances as soon as the write lands in page cache, not when it hits disk.

**The checkpoint is what confirms disk safety.** On a clean shutdown, the broker fsyncs everything and writes a checkpoint file per data directory: "I flushed up to offset X and the bytes are valid." On restart it trusts that receipt and recovers instantly.

On a crash (`kill -9`), no checkpoint is written. The broker wakes up, sees an offset in its metadata, but doesn't know if the bytes at that position actually made it to disk intact. So it scans the segment files byte-by-byte, finds the last complete valid message frame, and truncates everything after it. The offset rewinds to match what's actually on disk.

```
Claimed LEO (from metadata): offset 1042
Actual safe offset (after scan): offset 1038
→ last 4 messages were in page cache, never flushed, truncated
```

This scan is what `num.recovery.threads.per.data.dir` parallelizes — more threads means more partition directories scanned simultaneously on restart.

---

## Offsets in the Redpanda Console

In the Console group view you see one row per partition:

```
TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order.created  0          34              34              0
order.created  1          29              29              0
order.created  2          37              37              0
```

Three rows = three partitions. Each has its own independent bookmark. `LAG=0` across all means the group is fully caught up.

---

## Resetting offsets

```bash
# Stop all consumers in the group first (required)
pkill -f "services/consumer.py"

# Reset to the beginning
docker exec redpanda rpk group seek shipstream-consumer-group \
  --to start --topics order.created

# Reset to the end (skip all existing messages)
docker exec redpanda rpk group seek shipstream-consumer-group \
  --to end --topics order.created
```

---

> ← [Previous: Topics & Partitions](./topics-and-partitions.md) | [Index](../README.md) | [Next: Consumer Groups →](./consumer-groups.md)
