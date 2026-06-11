# Chapter 1 — What is Kafka?

> **You are here:** [Index](../README.md) → **What is Kafka?**

---

## The problem Kafka solves

Imagine an e-commerce platform where placing an order needs to trigger five things:

- Update inventory
- Send a confirmation email
- Notify the warehouse
- Update analytics
- Trigger fraud detection

The naive approach: the order service calls all five directly.

```
Order Service
    ├──→ Inventory Service   (HTTP)
    ├──→ Email Service       (HTTP)
    ├──→ Warehouse Service   (HTTP)
    ├──→ Analytics Service   (HTTP)
    └──→ Fraud Service       (HTTP)
```

This works until it doesn't. If the email service is slow, the order takes longer. If the warehouse service is down, the order fails. Every service is now a dependency of every other. This is **tight coupling**.

Kafka fixes this with a **broker** — a middleman that accepts messages and holds them until consumers are ready.

```
Order Service
    └──→ [order.created topic]
                ├──→ Inventory Service   (reads when ready)
                ├──→ Email Service       (reads when ready)
                ├──→ Warehouse Service   (reads when ready)
                ├──→ Analytics Service   (reads when ready)
                └──→ Fraud Service       (reads when ready)
```

The order service publishes once and moves on. It has no idea how many services are listening or whether they're even running right now.

---

## What Kafka actually is

Kafka is a **distributed message broker** — a durable, ordered, fault-tolerant log that producers write to and consumers read from.

Three things make it different from a traditional message queue:

1. **Messages are not deleted after consumption.** The log is immutable. Every consumer reads at its own pace and tracks its own position. You can have 10 different services all reading the same topic without stepping on each other.

2. **It is ordered.** Within a partition, messages are always in the order they were written. You can rely on this.

3. **It is durable.** Messages are written to disk and replicated. A consumer going offline for an hour comes back and reads everything it missed.

---

## Real-world Kafka users and why

| Industry | Use case |
|----------|---------|
| **Finance** | Stripe uses Kafka to stream payment events to fraud detection, ledger updates, and analytics simultaneously — all from one publish |
| **Ride-sharing** | Uber publishes every GPS ping from every driver to Kafka — routing, surge pricing, and ETAs all consume the same stream |
| **Streaming** | Netflix uses Kafka to track every play, pause, and seek event — recommendation engine and billing both read the same topic |
| **Retail** | Walmart publishes inventory changes to Kafka — replenishment, pricing, and the website all stay in sync |
| **Food delivery** | DoorDash uses Kafka to track order state transitions — kitchen, driver, and customer notification services each consume the same events |

---

## What is Redpanda?

Redpanda is a **Kafka-compatible drop-in replacement** written in C++ (Kafka is written in Java). It:

- Speaks the exact same Kafka wire protocol
- Works with all Kafka client libraries unchanged
- Is faster and simpler to operate (no ZooKeeper, no JVM tuning)
- Is what ShipStream runs in Docker

For learning purposes, everything in this knowledge base applies equally to Kafka and Redpanda.

---

## Key vocabulary

| Term | One-line definition |
|------|-------------------|
| **Broker** | The server — stores messages, serves producers and consumers |
| **Topic** | A named category of messages — like a table in a database |
| **Producer** | Writes messages to a topic |
| **Consumer** | Reads messages from a topic |
| **Consumer Group** | A named set of consumers sharing the work of reading a topic |

---

## In ShipStream

```
services/producer.py  →  [order.created]  →  services/consumer.py
```

The producer creates orders and publishes them. The consumer reads them and prints the details. One topic, one message type (`Order`), one consumer group — the simplest possible Kafka setup.

---

> **[→ Next: Topics & Partitions](./topics-and-partitions.md)**
