# Part 1 — Kafka

[← Back to Index](../README.md)

---

| Chapter | Topic | One-liner |
|---------|-------|-----------|
| 1 | [What is Kafka?](./what-is-kafka.md) | Why brokers exist, Kafka vs direct calls, Redpanda |
| 2 | [Topics & Partitions](./topics-and-partitions.md) | Append-only logs, partition keys, parallelism ceiling |
| 3 | [Offsets](./offsets.md) | Message addressing, log end offset, group offset, lag |
| 4 | [Consumer Groups](./consumer-groups.md) | Group mechanics, one-per-partition rule, multiple groups |
| 5 | [Producer](./producer.md) | Serialization, partition key, delivery guarantees |
| 6 | [Consumer](./consumer.md) | Poll loop, deserialization, offset commits |
| 7 | [Rebalancing](./rebalancing.md) | Partition redistribution, race conditions, stability |

---

**[→ Start with Chapter 1: What is Kafka?](./what-is-kafka.md)**

## Mental models

1. **Topic = immutable log, not a queue.** Messages survive being read. Multiple groups, same data.
2. **Partition count = parallelism ceiling.** You can never have more active consumers than partitions.
3. **One consumer per partition per group.** Kafka enforces this for ordering guarantees.
4. **Offset is per partition.** `topic + partition + offset` is the full address of any message.
5. **Group offset survives restarts.** Only the group ID matters, not which consumer instance.
6. **Multiple groups = independent reads.** Each group has its own bookmark on every partition.
