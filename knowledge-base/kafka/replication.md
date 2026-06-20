# Replication & Fault Tolerance

## Why Replication Exists

A single broker is a single point of failure. If its disk dies, or the process crashes, or the machine loses power, your data is gone and your pipeline stops.

Replication solves this by keeping identical copies of each partition on multiple brokers. When one broker dies, another already has the data and can take over — usually within a few seconds, with no messages lost.

The `brokers-and-clusters.md` chapter introduced replication at a surface level. This chapter covers the mechanics: how followers actually stay current, what happens when they fall behind, and how the cluster decides who becomes leader when the current one dies.

---

## The Leader-Follower Model

Every partition has exactly one **leader** at a time. The leader is the broker that:

- Accepts all writes from producers
- Serves all reads from consumers
- Tracks which followers are current

All other replicas are **followers**. A follower's only job is to fetch messages from the leader and write them to its own local log. Followers do not serve client requests — they are pure standby replicas.

```
order.created — Partition 0
  replication factor: 3

  Broker 1 (node 0) ← LEADER      accepts writes and reads
  Broker 2 (node 1) ← follower    fetches from leader continuously
  Broker 3 (node 2) ← follower    fetches from leader continuously
```

This is a pull model. Followers reach out to the leader; the leader does not push to followers. Each follower runs an internal fetch loop:

1. Send a `FetchRequest` to the leader: "give me messages starting from offset N"
2. Receive the batch
3. Write it to local disk
4. Repeat immediately

As long as the follower's fetch loop keeps up, it stays current. If it falls behind — because the broker is slow, the network is degraded, or the process is paused — it eventually loses its place in the ISR.

---

## ISR: In-Sync Replicas

The **ISR** (In-Sync Replicas) is the set of replicas that are considered current with the leader.

A follower is in the ISR if it has sent a fetch request to the leader within `replica.lag.time.max.ms` (default: 30 seconds in Kafka; Redpanda uses 30s as well but tracks it slightly differently). If a follower goes silent for longer than that window, the leader removes it from the ISR.

```
Partition 0 ISR — normal state
  Broker 1 (leader)  offset 10,450   ISR ✓
  Broker 2 (follower) offset 10,449  ISR ✓  (1 message behind — fine)
  Broker 3 (follower) offset 10,450  ISR ✓

Partition 0 ISR — broker 3 is lagging
  Broker 1 (leader)  offset 10,450   ISR ✓
  Broker 2 (follower) offset 10,449  ISR ✓
  Broker 3 (follower) offset 9,800   NOT in ISR (too far behind)

  ISR = [node 0, node 1]
```

A shrinking ISR is the earliest warning signal of a cluster problem. An ISR of `[0, 1, 2]` that suddenly becomes `[0, 1]` means either: broker 2 crashed, broker 2's disk is slow, or the network to broker 2 is degraded. The cluster will continue operating — but you've lost one fault-tolerance buffer.

**Where to see ISR state:**

```bash
docker exec redpanda-1 rpk topic describe order.created
```

Look for the `REPLICAS` and `OFFLINE-REPLICAS` columns in the partition table. You can also see it per-partition:

```bash
docker exec redpanda-1 rpk topic describe order.created --print-partitions
```

---

## replication.factor

`replication.factor` is set when you create a topic and determines how many copies of each partition exist across the cluster.

```bash
# Create a topic with RF=3 (requires at least 3 brokers)
docker exec redpanda-1 rpk topic create order.created \
  --partitions 3 \
  --replicas 3
```

The relationship between RF and fault tolerance is direct:

| replication.factor | Broker failures survived |
|---|---|
| 1 | 0 — any failure loses data |
| 2 | 1 |
| 3 | 2 |
| N | N-1 |

RF=3 is the standard production choice. It lets you survive one broker failure during normal operation plus one more during a rolling maintenance window — you're never left with RF=1 (which would be a single copy with no redundancy).

**You cannot set RF higher than your broker count.** Creating a topic with `--replicas 3` on a single-broker cluster is an error. This is why Phase 5 requires extending the docker-compose to 3 brokers before any simulation can run.

---

## min.insync.replicas

`min.insync.replicas` (often called `min.isr`) is the minimum number of ISR members that must acknowledge a write before the broker considers it successful — when the producer is using `acks=all`.

This is a **topic-level or broker-level** configuration. With RF=3 and `min.insync.replicas=2`:

```
Write arrives at leader (Broker 1)
  ↓
Leader writes to its own log
  ↓
Followers 2 and 3 both fetch and acknowledge
  ↓
ISR has 3 members ≥ min.isr of 2 → write succeeds, ack sent to producer
```

Now kill Broker 3:

```
Write arrives at leader (Broker 1)
  ↓
Leader writes to its own log
  ↓
Only Broker 2 acknowledges (Broker 3 is dead)
  ↓
ISR has 2 members = min.isr of 2 → write still succeeds
```

Now kill Broker 2 as well:

```
Write arrives at leader (Broker 1)
  ↓
Only leader has written
  ↓
ISR has 1 member < min.isr of 2 → write FAILS
  ↓
Producer receives: NOT_ENOUGH_REPLICAS
```

This is intentional. If you allowed writes with only 1 ISR member and that member died before a follower caught up, you'd lose confirmed messages. The `NOT_ENOUGH_REPLICAS` error tells the producer: "we can't safely commit this write right now, try again later."

**The standard configuration:**

```
replication.factor = 3
min.insync.replicas = 2
acks = all  (producer-side)
```

This gives you: durability (you can lose 1 broker with no data loss), availability (you can lose 1 broker and still accept writes), and a clear failure signal (losing 2 brokers halts writes rather than silently proceeding with a single copy).

**Setting min.insync.replicas:**

```bash
# At topic level (overrides broker default)
docker exec redpanda-1 rpk topic alter-config order.created \
  --set min.insync.replicas=2

# Check current config
docker exec redpanda-1 rpk topic describe order.created --print-configs
```

---

## Leader Election

When the leader broker dies, the cluster needs to pick a new leader for every partition it was leading. This is handled by the **controller** — one broker per cluster that is elected to manage metadata operations.

In Kafka (and Redpanda), KRaft replaced ZooKeeper for controller election. The controller is a broker that won the controller election via the Raft protocol. In Redpanda, all three brokers participate in a Raft group for cluster metadata.

**The election rule is strict: only ISR members can become the new leader.**

```
Before: Broker 1 dies
  Partition 0 leader: Broker 1
  ISR: [Broker 1, Broker 2, Broker 3]

After: controller elects a new leader
  Controller looks at ISR (minus the dead broker): [Broker 2, Broker 3]
  Picks one (usually the first in the list): Broker 2
  Partition 0 leader: Broker 2
  ISR: [Broker 2, Broker 3]
```

Because Broker 2 was in the ISR, it has all messages up to the last ack'd offset. Producers reconnect to Broker 2 and continue from exactly where they left off. Consumers do the same. No messages are lost.

The key point: **if the ISR only contained the leader at the moment it died, there is no eligible follower.** No election can happen. The partition goes offline until either the dead broker comes back or you enable unclean leader election.

---

## Unclean Leader Election

`unclean.leader.election.enable` (default: `false`) controls whether an out-of-ISR replica can become leader when no ISR member is alive.

**Why you'd want it:** availability. If all ISR members are dead and you have a stale follower, enabling unclean election means the stale follower becomes leader and the partition comes back online — at the cost of losing whatever messages it hadn't fetched before the ISR died.

```
Scenario: leader + 1 follower both die (ISR is now empty)
  Broker 3 (stale follower) is still alive
  It was at offset 9,800 when the others died
  The leader had reached offset 10,450

Without unclean election (default):
  Partition is OFFLINE — no leader, no reads or writes
  Producers get LeaderNotAvailableException
  Must wait for a dead broker to come back

With unclean.leader.election.enable=true:
  Broker 3 becomes leader
  Partition comes back online at offset 9,800
  Messages 9,800–10,449 are GONE — silently lost
  Producers that got acks for those messages are now lied to
```

**When to use it:**
- Analytics pipelines where occasional data loss is acceptable
- Topics that can be replayed from source (e.g., a changelog that can be rebuilt)
- When availability is more important than durability

**When not to use it:**
- Financial transactions
- Inventory updates
- Anything where a confirmed write must not disappear

The default (`false`) is the right production choice for most systems. An offline partition is visible and fixable. Silent data loss is not.

---

## replica.fetch.max.bytes

The Phase 4 chapter introduced a three-way size alignment:

```
max.request.size (producer)
  ≤ message.max.bytes (broker)
    ≤ replica.fetch.max.bytes (broker)
      ≤ max.partition.fetch.bytes (consumer)
```

`replica.fetch.max.bytes` is the maximum bytes a follower can fetch from the leader in a single request. If a leader has a message batch larger than this, the follower can't fetch it and falls behind.

The rule: **`replica.fetch.max.bytes` must be ≥ `message.max.bytes`.**

Redpanda's defaults are generous (both default to 1 MiB in standard Kafka, much higher in Redpanda), but if you raise `message.max.bytes` to allow large messages, you must raise `replica.fetch.max.bytes` at the same time or followers will stall and fall out of the ISR.

---

## What Happens in Practice

Here is the full timeline when a leader broker dies mid-production:

```
t=0    Broker 1 is leader for partition 0. ISR = [0, 1, 2].
       Producer is sending 100 orders/s with acks=all.

t=5s   Broker 1 crashes (container killed).

t=5s   Followers 2 and 3 attempt to fetch → connection refused.
       They report the failed heartbeat to the controller.

t=6s   Controller detects Broker 1 is gone via missed metadata heartbeats.
       Controller elects Broker 2 as the new leader for partition 0.
       Controller broadcasts metadata update to all brokers.

t=6s   Producer's next produce attempt gets leader-not-available error.
       confluent-kafka retries automatically (default retry behavior).
       Producer fetches updated metadata → sees Broker 2 is the new leader.
       Producer resumes sending to Broker 2.

t=6-7s  A few produce calls may fail and be retried. With acks=all and
        retries enabled, no messages are lost. With acks=1, any messages
        in-flight to Broker 1 at the moment of crash are lost.

t=7s   Production is fully resumed. ISR = [1, 2].
       Partition 0's replication factor is still 3, but only 2 are in ISR.
       This is "under-replicated" — visible in broker metrics.
```

The failover takes seconds. The pipeline continues. This is the core promise of a replicated Kafka cluster.

---

## Next

[Phase 5 Simulations: S9, S10, S11 →](../simulations/phase5-guide.md)
