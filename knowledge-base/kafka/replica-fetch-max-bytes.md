# replica.fetch.max.bytes

## What it is

When a follower replicates from the leader, it sends a fetch request: *"give me messages starting from offset N."* The leader responds with a batch of messages. `replica.fetch.max.bytes` is the size cap on that response — the maximum the leader will send back in a single fetch.

It lives on the broker, alongside `message.max.bytes`. Both are broker-level settings that must be kept in sync.

---

## Why it needs to match message.max.bytes

`message.max.bytes` is the maximum size of a single message the broker will accept from a producer.

Now imagine this mismatch:

```
message.max.bytes       = 10 MB   ← broker accepts messages up to 10 MB
replica.fetch.max.bytes =  1 MB   ← follower can only fetch 1 MB at a time
```

A producer sends a 5 MB message. The leader accepts it — it is within `message.max.bytes`. The follower then tries to fetch it, but the response would be 5 MB, which exceeds its `replica.fetch.max.bytes` of 1 MB. The follower gets stuck at that offset. It falls further and further behind, eventually getting dropped from the ISR.

You now have a message sitting on the leader that no follower can ever replicate. Your RF=3 topic is silently running at RF=1 — with zero alerts, zero errors on the producer side, and no visible failure until the leader dies.

---

## The four-way size chain

Every message passes through four gates. Each gate must be at least as large as the one before it:

```
max.request.size              (producer side)
  ≤ message.max.bytes         (broker — accepts the write)
    ≤ replica.fetch.max.bytes (broker — follower pulls from leader)
      ≤ max.partition.fetch.bytes (consumer side)
```

| Setting | Where | What it gates |
|---|---|---|
| `max.request.size` | producer config | max size of a single produce request the client will send |
| `message.max.bytes` | broker config | max size the broker will accept and store |
| `replica.fetch.max.bytes` | broker config | max batch a follower can pull from the leader in one fetch |
| `max.partition.fetch.bytes` | consumer config | max batch a consumer can pull per partition in one fetch |

If any gate is narrower than the one before it, messages that fit through the earlier gate get stuck at the narrower one.

Phase 4 introduced the first three gates in the context of log retention. This chapter adds `replica.fetch.max.bytes` as the third gate — the one that lives between the broker accepting a write and a follower being able to replicate it.

---

## When the defaults are fine

Redpanda's defaults are generous and the chain is already aligned out of the box. You do not need to touch any of these settings for the standard ShipStream pipeline — orders are small Protobuf messages, well under 1 MB.

The mismatch problem only appears when you raise `message.max.bytes` to support large payloads (images, blobs, bulk exports). At that point, you must raise `replica.fetch.max.bytes` by the same amount at the same time, or the first large message will silently stall your followers.

---

## The rule

```
replica.fetch.max.bytes ≥ message.max.bytes
```

Change one, change the other. That's the entire operational contract.

---

## Related

- [Replication & Fault Tolerance](./replication.md) — ISR, leader election, unclean election
- [Log Retention & Storage](./log-retention.md) — where the three-way chain was first introduced
