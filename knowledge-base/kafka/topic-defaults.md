# Broker Configuration: Topic Defaults

These four settings sit quietly in your broker config and silently shape every topic that gets created. They're easy to ignore in development, expensive to misconfigure in production.

The examples in this chapter use a **payment events** pipeline — `payment.initiated`, `payment.captured`, and `payment.failed` topics — to make the tradeoffs concrete.

---

## log.dirs

This is where the broker stores all partition data on disk.

```properties
log.dirs=/var/kafka/data
```

Every partition is a directory inside `log.dirs`. Inside that directory are **segment files** — the actual append-only log files where messages are written. For example:

```
/var/kafka/data/
  payment.initiated-0/       ← partition 0 of the topic
    00000000000000000000.log  ← segment file (messages 0–N)
    00000000000000000000.index
    00000000000000000000.timeindex
  payment.initiated-1/
    ...
  payment.initiated-2/
    ...
```

**Multiple directories.** You can specify a comma-separated list to spread data across multiple disks:

```properties
log.dirs=/mnt/disk1/kafka,/mnt/disk2/kafka,/mnt/disk3/kafka
```

When you have multiple directories, Kafka distributes new partitions across them round-robin — not by available space, by count. If one disk fills up but the others haven't, Kafka still tries to write to the full disk for partitions already assigned there. This means you want disks of equal size, not a mix of old and new drives.

**Why it matters for payments.** A high-throughput payment processor might see 50,000 `payment.initiated` events per second. That's a lot of disk I/O. Pointing `log.dirs` at a single slow disk is a silent bottleneck — messages batch up in the producer, latency rises, and you see lag accumulate on the consumer side without any obvious error.

---

## num.recovery.threads.per.data.dir

```properties
num.recovery.threads.per.data.dir=1
```

When a broker starts (or restarts after a crash), it needs to scan its log segments to verify their integrity and find the correct end offset. This is called log recovery.

This setting controls how many threads do that scanning **per data directory**. If you have 3 directories and set this to 2, you get 6 recovery threads total.

**Clean shutdown vs. crash.** If the broker was cleanly shut down, recovery is fast — it just reads a small checkpoint file. If it crashed (power loss, OOM kill, `kill -9`), it has to scan the last uncommitted segments byte-by-byte to find where the log is actually clean.

**Tuning guidance.** The default of 1 is fine if you have a small number of partitions. If you have hundreds of partitions across a few disks, a crash recovery can take minutes. Setting this to 4–8 (matching your disk count or CPU count, whichever is smaller) can cut recovery time significantly.

For a payment pipeline where downtime directly means failed transactions, reducing broker restart time matters. Set `num.recovery.threads.per.data.dir` equal to your disk count as a starting point.

---

## auto.create.topics.enable

```properties
# Development default (often true)
auto.create.topics.enable=true

# Production recommendation
auto.create.topics.enable=false
```

When this is `true`, any producer that sends to a topic that doesn't exist will automatically create it with default settings (`num.partitions`, default replication factor). Same for any consumer that subscribes to a nonexistent topic.

**Why this is dangerous in production.**

Consider a payment service with a typo:

```python
producer.produce("payment.initated", ...)  # typo: "initated"
```

With `auto.create.topics.enable=true`, Kafka silently creates a new topic called `payment.initated` with default settings (likely 1 partition, replication factor 1). Your real `payment.initiated` consumers see nothing. The payment events disappear into a ghost topic. No error. No alert. Just silent data loss.

With `auto.create.topics.enable=false`, the producer gets an error immediately: `UNKNOWN_TOPIC_OR_PARTITION`. The typo is caught at deploy time, not discovered three hours later when someone notices the ledger is missing transactions.

**The discipline it enforces.** Disabling auto-create forces you to provision topics explicitly — via `rpk topic create`, Terraform, or a CI step. This means you consciously choose the partition count and replication factor for each topic rather than inheriting whatever the broker default happens to be.

---

## num.partitions

```properties
num.partitions=1
```

This is the **default** partition count applied to any topic that is created without an explicit partition count — either via auto-creation or when you run `rpk topic create` without the `--partitions` flag.

This is one of the most consequential settings to get right, and one of the hardest to change after the fact.

**Why you can't easily change partition count.** Kafka allows you to *increase* partitions on an existing topic, but you cannot decrease them. More importantly, increasing partitions after data is already being produced breaks the ordering guarantee for keyed messages.

If your payment producer uses `payment_id` as the partition key, then all events for `payment-abc-123` go to the same partition (and therefore the same consumer). This guarantees in-order processing for that payment. If you add a partition, the hash of `payment-abc-123` might now map to a different partition — so some consumers have the early events and a different consumer gets the later ones. Depending on how your consumer logic works, this can cause out-of-order processing.

**Getting the count right at creation time** is far less painful than fixing it later.

**How to choose.** The partition count sets the maximum consumer parallelism for that topic. A topic with 6 partitions can be consumed by at most 6 consumers in the same group at the same time. Beyond 6, extras sit idle.

For the payment pipeline, think about your consumer throughput:

- `payment.initiated` — high volume, latency-sensitive, want maximum parallelism → 12 partitions
- `payment.failed` — much lower volume, one or two consumers is fine → 3 partitions
- `payment.captured` — feeds ledger writes, which must be ordered per account → 6 partitions keyed by `account_id`

A reasonable production default is `num.partitions=3`. It's better than 1 (you can run 3 consumers in parallel), and it doesn't force you to run 12 consumers to keep up when your load is modest.

**The relationship with throughput.** More partitions also means more open file handles, more memory for fetches, and more overhead on the broker. There's a point of diminishing returns. For most teams, the partition count discussion is about matching consumer concurrency, not about raw throughput.

---

## Summary

| Setting | Safe dev default | Production recommendation |
|---|---|---|
| `log.dirs` | single directory | multiple directories across disks |
| `num.recovery.threads.per.data.dir` | 1 | match disk count |
| `auto.create.topics.enable` | `true` | `false` |
| `num.partitions` | 1 | 3 (or per-topic via explicit create) |

The pattern across all four: development rewards convenience (single dir, auto-create, default 1), production rewards explicitness (managed disk I/O, topic provisioning CI, conscious partition sizing).

---

## Next

[Chapter — Log Retention & Storage Configuration →](./log-retention.md)
