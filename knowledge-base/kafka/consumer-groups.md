# Chapter 4 — Consumer Groups

> **You are here:** [Index](../README.md) → [Offsets](./offsets.md) → **Consumer Groups**

---

## What is a consumer group?

A **consumer group** is a named set of consumer instances that cooperate to read a topic. Kafka distributes partitions across the members so that:

- Every partition is owned by exactly **one** consumer in the group
- Every message is processed by exactly **one** consumer in the group
- If a consumer dies, its partitions are reassigned to the survivors

The group is identified by a string `group.id`. Kafka tracks the group's offset per partition server-side.

---

## The partition assignment rule

The rule is often stated as "one consumer per partition" but the direction matters:

> **A partition can be held by at most one consumer member at a time. A single consumer member can hold multiple partitions.**

```
3 partitions, 1 consumer → consumer holds all 3
┌─────────────┐
│  Consumer 1 │ ← partition 0
│             │ ← partition 1
│             │ ← partition 2
└─────────────┘

3 partitions, 3 consumers → 1 partition each (the ideal)
┌──────────┐  ┌──────────┐  ┌──────────┐
│Consumer 1│  │Consumer 2│  │Consumer 3│
│  part. 0 │  │  part. 1 │  │  part. 2 │
└──────────┘  └──────────┘  └──────────┘

3 partitions, 4 consumers → 4th sits idle (no partition left)
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│Consumer 1│  │Consumer 2│  │Consumer 3│  │Consumer 4│
│  part. 0 │  │  part. 1 │  │  part. 2 │  │  (idle)  │
└──────────┘  └──────────┘  └──────────┘  └──────────┘
```

**Why can't two consumers share one partition?** Ordering. If two consumers read the same partition simultaneously there is no way to guarantee messages are processed in sequence, and offset tracking becomes ambiguous — which consumer's commit wins? One consumer owns one partition exclusively = strict ordering within that partition, unambiguous offset progression.

The idle consumer in the 4-consumer example is not wasted — it acts as a hot standby. If Consumer 1 dies, the broker triggers a rebalance and the idle consumer immediately picks up partition 0.

---

## Scaling with consumer groups

To handle more throughput, add consumers up to the partition count. Beyond that, create more partitions (at topic creation time).

| Partitions | Max useful consumers | Pattern |
|-----------|---------------------|---------|
| 1 | 1 | Development / low traffic |
| 3 | 3 | Small production |
| 12 | 12 | Medium production |
| 100 | 100 | High-throughput (Uber, Netflix scale) |

**Industry examples:**

| Industry | Group name | Consumers | Why that many |
|----------|-----------|-----------|--------------|
| **Payments** | `fraud-detection-group` | 50 | Each payment must be checked in <100ms; needs massive parallelism |
| **Logistics** | `warehouse-assignment-group` | 10 | Each order needs a warehouse lookup; moderate latency ok |
| **Streaming** | `recommendation-group` | 200 | Billions of play events per day; needs extreme parallelism |
| **Retail** | `inventory-sync-group` | 5 | Inventory updates are infrequent; light load |

---

## Multiple groups = independent reads

Different services that need the same events use different `group.id` values. They each get their own independent offset pointer on every partition.

```
Topic: order.created
         │
         ├──────────────────────────────────────────────
         │                                              │
inventory-consumer-group                  analytics-consumer-group
  partition-0 → offset 200                  partition-0 → offset 50
  partition-1 → offset 195                  partition-1 → offset 48
  partition-2 → offset 198                  partition-2 → offset 51
  (fully caught up)                         (batch processing, behind by design)
```

Publishing one order event → both groups eventually process it. Neither group knows the other exists.

**Real-world example — Uber:**
A single `driver.location.updated` topic is consumed by:
- `routing-group` — updates the driver's position for dispatch decisions
- `eta-group` — recalculates arrival times for riders
- `surge-pricing-group` — tracks supply density per zone
- `analytics-group` — feeds the data warehouse

Four completely independent services, one topic, four group offsets.

---

## group.id and client.id

```python
consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": "shipstream-consumer-group",           # which group this instance belongs to
    "client.id": f"shipstream-consumer-{CONSUMER_ID}", # identifies THIS instance in the UI
    "auto.offset.reset": "earliest",
})
```

These two fields are often confused but serve completely different purposes:

**`group.id`** — the broker uses this for everything functional:
- Partition assignment: all members with the same `group.id` split the partitions between them
- Offset tracking: committed offsets are stored per group, per partition on the broker

**`client.id`** — a human-readable label for this specific process. The broker uses it for nothing functional. It shows up in logs, Redpanda Console, and `rpk group describe` under the CLIENT-ID column so you can tell consumers apart.

```
group.id  = "shipstream-consumer-group"   → which team you're on
client.id = "shipstream-consumer-3"       → your name tag within the team
```

The practical consequence:

```
Same group.id, different client.id
  → broker treats them as one group, splits partitions between them
  → offsets are shared — one member's commit advances the group pointer

Different group.id, same client.id (unusual but valid)
  → broker treats them as independent groups, each reads the full topic
  → offsets are completely separate
```

**`group.id` is what matters for failover.** If a consumer crashes and a new process starts with the same `group.id` (even with a different `client.id`), the broker assigns it the same partitions and it picks up from the last committed offset. `client.id` plays no role in this.

`auto.offset.reset` — what to do when the group has no committed offset yet:
- `"earliest"` — read from the very beginning of the log
- `"latest"` — skip all existing messages, only read new ones

---

> ← [Previous: Offsets](./offsets.md) | [Index](../README.md) | [Next: Producer →](./producer.md)
