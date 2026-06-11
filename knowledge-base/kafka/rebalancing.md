# Chapter 7 — Rebalancing

> **You are here:** [Index](../README.md) → [Consumer](./consumer.md) → **Rebalancing**

---

## What is a rebalance?

When the membership of a consumer group changes, Kafka **redistributes all partitions** across the current members. This is called a rebalance.

During a rebalance, **all consumers in the group pause** — no messages are processed until the new assignment is settled. This is called a stop-the-world rebalance (Kafka 3.x introduced incremental cooperative rebalancing to reduce this pause, but the default is still stop-the-world).

---

## When rebalances trigger

| Event | Trigger |
|-------|---------|
| A new consumer joins the group | `subscribe()` call completes |
| A consumer calls `close()` | Clean shutdown sends `LeaveGroup` |
| A consumer stops heartbeating | Session timeout expires (default 45s) |
| Topic partitions are added | New partitions need owners |

---

## What happens step by step

```
Initial state: 3 partitions, Consumer-1 running alone
─────────────────────────────────────────────────────────────
Consumer-1 owns: P0, P1, P2

Consumer-2 starts and calls subscribe()
→ REBALANCE TRIGGERED
  All partitions revoked from Consumer-1
  Kafka re-assigns:
    Consumer-1 → P0, P1
    Consumer-2 → P2
  Processing resumes

Consumer-3 starts
→ REBALANCE TRIGGERED AGAIN
  All partitions revoked
  Kafka re-assigns:
    Consumer-1 → P0
    Consumer-2 → P1
    Consumer-3 → P2
  Processing resumes

Consumer-2 crashes (no heartbeat for 45s)
→ REBALANCE TRIGGERED
  Consumer-2's P1 revoked
  Kafka re-assigns P1 to Consumer-1 or Consumer-3
```

---

## The race condition we hit

When we launched 3 consumers simultaneously with `&`:

```bash
for i in 1 2 3; do CONSUMER_ID=$i python3 -u services/consumer.py & done
```

The OS decided which process got CPU first. Consumer-2 won the scheduling lottery, connected to Redpanda, joined the group, and was assigned all 3 partitions before Consumer-1 or Consumer-3 even finished their TCP handshake.

```
Wall clock (approximate)
──────────────────────────────────────────────────────
t=0ms    Shell fires Consumer-1 (PID 66302) in background
t=1ms    Shell fires Consumer-2 (PID 66303) in background
t=2ms    Shell fires Consumer-3 (PID 66304) in background

t=??ms   OS scheduler gives CPU to Consumer-2 first
         Consumer-2: TCP connect → JoinGroup → assigned P0, P1, P2
         Consumer-2: starts reading from offset 0
         Consumer-2: reads 102 messages (offsets 0–101)

t=??ms   Consumer-1 finally connects → REBALANCE
         Consumer-1 gets P0, P1, P2 (Consumer-2 had read everything)
         Consumer-1: reads new messages from producer (offsets 102–201)

t=??ms   Consumer-3 connects → REBALANCE
         3 partitions split across 3 consumers
         Consumer-3: nothing left to read
```

Result: Consumer-2 got all old messages, Consumer-1 got all new messages, Consumer-3 got nothing. If you ran it again, Consumer-1 might win the race instead. It's non-deterministic.

---

## The fix: wait for group stability

```bash
# Start consumers
for i in 1 2 3; do
  CONSUMER_ID=$i python3 -u services/consumer.py > logs/consumers/consumer-$i.log 2>&1 &
done

# Wait for all 3 to join and the final rebalance to complete
sleep 8

# Verify the group is stable before publishing
docker exec redpanda rpk group describe shipstream-consumer-group
# Look for: MEMBERS=3, STATE=Stable

# Now publish
python3 services/producer.py
```

With a stable group and 3 partitions, 100 messages should spread roughly evenly: ~33-34 per consumer.

---

## Industry impact of rebalances

Rebalances are not free. During a rebalance, the entire consumer group pauses. For high-throughput systems this matters:

| Industry | Impact of a 5-second rebalance |
|----------|-------------------------------|
| **Payments** | 5 seconds of payments not being fraud-checked — risky |
| **Ride-sharing** | 5 seconds of GPS pings not routing drivers — degraded dispatch |
| **Retail** | 5 seconds of orders not hitting inventory — minor delay |
| **Analytics** | 5 seconds of events buffered — usually fine |

Strategies to minimize rebalance impact:
- Set `session.timeout.ms` conservatively (not too short or you get false positives)
- Use `max.poll.interval.ms` to give slow consumers more time before being declared dead
- Design consumers to be stateless so reassignment is cheap
- Use Kafka's incremental cooperative rebalancing (Kafka 3.x) to avoid stop-the-world pauses

---

> ← [Previous: Consumer](./consumer.md) | [Index](../README.md) | [Next: What is Protobuf? →](../protobuf/what-is-protobuf.md)
