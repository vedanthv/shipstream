# Brokers and Clusters

## What Is a Broker?

A **broker** is a single Kafka server process — one machine (or container) running the Kafka daemon. Its job is simple in principle:

- Accept messages from producers and write them to disk
- Accept fetch requests from consumers and return messages
- Track topic metadata (which partitions exist, where their replicas live)

That's it. A broker doesn't run your application code. It doesn't understand what a payment is. It just appends bytes to log files and serves them back.

When you ran `docker compose up -d` in Phase 1, you started one broker. That single process handled everything: writes, reads, and metadata. This is fine for development. It is not fine for production.

---

## What Is a Cluster?

A **cluster** is two or more brokers that coordinate together to form a single logical Kafka deployment.

Here is why you need more than one:

**Fault tolerance.** If your single broker crashes, your entire pipeline stops — producers can't write, consumers can't read. With three brokers, one can go down and the pipeline continues.

**Throughput.** A single disk can only do so much I/O. Spreading partitions across multiple brokers (each with their own disk) multiplies your total write throughput.

**Replication.** Kafka copies each partition to multiple brokers. The broker currently accepting writes for a partition is the **leader**. The others holding copies are **followers** (also called replicas). If the leader dies, Kafka elects a follower as the new leader. This is transparent to producers and consumers.

```
Payment Topic — 3 partitions, replication factor 3

            Broker 1        Broker 2        Broker 3
Partition 0  LEADER         follower        follower
Partition 1  follower       LEADER          follower
Partition 2  follower       follower        LEADER
```

Each broker is the leader for some partitions and a follower for others. The load is spread across the cluster.

---

## broker.id

Every broker in a cluster must have a unique integer ID. This is the `broker.id` setting.

```properties
# broker 1
broker.id=1

# broker 2
broker.id=2

# broker 3
broker.id=3
```

The broker ID is how the cluster tracks:
- Which broker owns which partition replicas
- Which broker is the current leader for a partition
- Which broker is in the ISR (in-sync replicas) list

**What happens if two brokers share the same ID?** The second broker to register will be rejected by the cluster controller. Partitions previously assigned to the original broker with that ID go into an unassigned state. The cluster is degraded until the conflict is resolved.

This matters most when you're automating broker provisioning. If your infrastructure-as-code accidentally spins up two nodes with `broker.id=2`, you have a split-brain situation until someone intervenes.

In Redpanda, `broker.id` is called `node_id` in config files, but the concept is identical.

---

## listeners and advertised.listeners

This is where most people get confused the first time they deploy Kafka in Docker or behind a load balancer.

There are two separate addresses:

**`listeners`** — the address the broker process actually **binds to** (listens on). This is what the OS uses.

**`advertised.listeners`** — the address the broker tells **clients** to use. This is what producers and consumers receive when they ask "which broker handles partition X?"

Why are they different? Consider this Docker setup:

```
┌─────────────────────────────────┐
│  Docker network (172.17.0.x)    │
│                                 │
│   ┌──────────────────────────┐  │
│   │  Redpanda container      │  │
│   │                          │  │
│   │  binds to: 0.0.0.0:9092  │  │
│   │  (all interfaces)        │  │
│   └──────────────────────────┘  │
│                                 │
└─────────────────────────────────┘
         │
         │  port-mapped to
         ▼
   host machine: localhost:19092
```

The broker binds to `0.0.0.0:9092` inside the container. That's `listeners`.

A client running on your laptop connects to `localhost:19092` (the port-mapped address). The broker responds to the initial connection — but then it sends back broker metadata saying "for partition 0, connect to `<broker address>`." If it says `172.17.0.2:9092` (the container-internal address), your laptop client can't reach that. The connection breaks.

`advertised.listeners` is the fix. You set it to `localhost:19092` (the address your clients can actually reach), and the broker hands that address out in metadata responses, not its internal bind address.

```properties
# Inside the container — what the OS listens on
listeners=PLAINTEXT://0.0.0.0:9092

# What the broker tells clients to connect to
advertised.listeners=PLAINTEXT://localhost:19092
```

**The payment pipeline example.** Imagine you have a payment processor running in a Kubernetes pod that publishes `payment.initiated` events. The Kafka broker is in a different cluster. If `advertised.listeners` is set to the broker's pod IP and that pod gets rescheduled, the IP changes, and all your payment producers get stale metadata. Setting it to a stable service hostname (`kafka-broker-1.kafka.svc.cluster.local`) makes the address stable regardless of where the pod lands.

You can also define multiple listener types to separate internal and external traffic:

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093
advertised.listeners=INTERNAL://broker1.internal:9092,EXTERNAL://payments.example.com:9093
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:SSL
```

Internal services (consumers, other brokers) talk over the fast internal listener. External clients (a payment SaaS webhook) connect through the SSL external listener.

---

## Replication Factor vs. Partition Count

These are the two most important numbers when creating a topic and they serve completely different purposes.

**Partition count** — how many parallel slices the topic is divided into. This sets the ceiling on consumer parallelism. A `payment.initiated` topic with 6 partitions can be consumed by at most 6 consumers in the same group simultaneously.

**Replication factor** — how many copies of each partition exist across brokers. A replication factor of 3 means each partition has one leader and two followers. You need at least 3 brokers to use a replication factor of 3.

```
payment.initiated topic
  partitions: 6
  replication factor: 3

  → 6 partition leaders spread across brokers
  → 6 × 2 = 12 follower replicas also spread across brokers
  → total: 18 partition-replica slots distributed across the cluster
```

The rule of thumb: **replication factor = number of broker failures you can survive**. Factor of 1 means any single broker failure causes data loss. Factor of 3 means you can lose 2 brokers and still serve reads and writes.

---

## The ISR (In-Sync Replicas)

Not all replicas are equal. A follower is **in-sync** if it has caught up with the leader within a configured time window (`replica.lag.time.max.ms`, default 30 seconds). The set of brokers that are in-sync is the ISR.

```
Partition 0 for payment.initiated
  Leader:   Broker 1 (offset 10,004)
  Follower: Broker 2 (offset 10,002) ← in ISR
  Follower: Broker 3 (offset 9,800)  ← NOT in ISR (too far behind)

  ISR = [1, 2]
```

Why does this matter? When a producer sends a payment event with `acks=all`, the leader waits for **all ISR members** to acknowledge the write before confirming success to the producer. Broker 3 being behind doesn't block the write — it's just excluded from the ISR until it catches up.

If the leader (Broker 1) dies, only Broker 2 is eligible to be elected as the new leader. Broker 3 could potentially be elected if you set `unclean.leader.election.enable=true`, but that risks data loss — Broker 3 may not have the last 200 messages.

---

## Putting It Together

Here is what happens when you run three payment-processing services:

```
payment-service (producer)
    │
    │  publishes payment.initiated
    ▼
┌─────────────────────────────────────────┐
│         Kafka Cluster (3 brokers)        │
│                                         │
│  Broker 1 ←── leader for partition 0   │
│  Broker 2 ←── leader for partition 1   │
│  Broker 3 ←── leader for partition 2   │
│                                         │
│  Each broker holds follower replicas    │
│  for the other two partitions           │
└─────────────────────────────────────────┘
         │          │          │
         ▼          ▼          ▼
  fraud-checker  ledger-writer  audit-logger
  (consumer 1)   (consumer 2)  (consumer 3)
```

Each consumer in the `payment-processor-group` owns one partition. All three can run in parallel. If Broker 2 goes down, the follower on Broker 1 (or 3) is elected leader for partition 1. The `ledger-writer` consumer reconnects to the new leader, refreshes its metadata, and continues from its committed offset — no messages lost, a few seconds of reconnection delay.

This is the fundamental reliability guarantee of a Kafka cluster: the data survives broker failures, and consumers resume where they left off.

---

## Next

[Chapter — Broker Configuration: Topic Defaults →](./topic-defaults.md)
