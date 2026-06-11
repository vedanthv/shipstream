# Chapter 8 — What is Protobuf?

> **You are here:** [Index](../README.md) → [Rebalancing](../kafka/rebalancing.md) → **What is Protobuf?**

---

## The serialization problem

Every time two services exchange data, someone has to answer: *how do bytes become a structured object and back again?*

The naive answer is JSON:

```json
{
  "id": "abc-123",
  "customer_id": "customer-42",
  "item": "Mechanical Keyboard",
  "amount": 149.99,
  "status": "ORDER_STATUS_CREATED"
}
```

This works fine until it doesn't. JSON carries the field **names** in every single message. `"customer_id"` is 13 bytes of overhead, repeated millions of times per minute. At Uber's scale (1M+ GPS pings/min), that's gigabytes of field names crossing the network every hour — pure waste.

---

## What Protobuf does instead

Protocol Buffers (Protobuf) is a **binary serialization format** built on one insight: both sides already know the schema. So instead of repeating field names, just send a number.

```
JSON message (≈ 93 bytes)
─────────────────────────────────────────────────────────────────
{ "id": "abc", "customer_id": "c-42", "item": "Keyboard", 
  "amount": 149.99, "status": "ORDER_STATUS_CREATED" }

Protobuf message (32 bytes — same data)
─────────────────────────────────────────────────────────────────
0a 03 61 62 63                   ← field 1 (id): "abc"
12 04 63 2d 34 32               ← field 2 (customer_id): "c-42"
1a 08 4b 65 79 62 6f 61 72 64  ← field 3 (item): "Keyboard"
21 48 e1 7a 14 ae bf 62 40     ← field 4 (amount): 149.99
28 01                           ← field 5 (status): 1 (CREATED)
```

No field names. No quotes. No braces. Just field numbers and values. Both producer and consumer have the schema — they know that field `2` is `customer_id`.

---

## Side-by-side encoding comparison

```
         JSON                          Protobuf
┌────────────────────────┐    ┌──────────────────────┐
│ "customer_id": "c-42" │    │  0x12  0x04  "c-42" │
│  ─────────────────────│    │  tag   len   value   │
│  13 bytes for the key  │    │  1 byte for the key  │
│   1 byte colon         │    │  1 byte for length   │
│   7 bytes for value    │    │  4 bytes for value   │
│   2 bytes for quotes   │    └──────────────────────┘
│   1 byte comma         │         6 bytes total
│  ─────────────────────│
│   24 bytes total       │
└────────────────────────┘
```

For this single field alone: **4× smaller** in Protobuf.

---

## JSON vs Protobuf at a glance

| Property | JSON | Protobuf |
|----------|------|----------|
| Format | Human-readable text | Binary bytes |
| Field identification | String names (`"customer_id"`) | Integer numbers (`2`) |
| Size | ~93 bytes (Order example) | ~32 bytes (same data) |
| Parse speed | Text parsing — slow | Binary parsing — fast |
| Schema | Optional, not enforced | Required, strictly enforced |
| Type safety | Loose — `"149.99"` and `149.99` look different but could be either | Strict — `double` is always 8 bytes of IEEE 754 |
| Human readable | Yes | No — needs schema to decode |
| Backwards compatible | Manual | Built-in via field numbers |

---

## The field number is the contract

This is the most important concept in Protobuf:

```protobuf
message Order {
  string id          = 1;   ← number 1 travels on the wire, not "id"
  string customer_id = 2;   ← number 2 travels on the wire, not "customer_id"
  string item        = 3;
  double amount      = 4;
  OrderStatus status = 5;
}
```

The name `customer_id` exists only in your `.proto` file and your generated code. It is compiled away. Rename it to `buyer_id` tomorrow — the wire format is identical, old consumers keep working.

Change the number from `2` to `7` — every consumer that hasn't deployed breaks silently, reading the wrong data from field 7.

**Names are for humans. Numbers are for machines.**

---

## Industry adoption

| Company | Volume | Why Protobuf |
|---------|--------|-------------|
| **Google** | Trillions of internal RPCs/day | Invented it. All internal services use it |
| **Uber** | ~1M driver location updates/min | JSON would add 200+ GB/day of field-name overhead |
| **Netflix** | Billions of play events/day | Schema enforcement between 1000+ microservices |
| **Square** | Millions of payments/day | `double amount` can never accidentally become a string |
| **Dropbox** | Petabytes of metadata | 3× smaller = real infrastructure cost savings |
| **LinkedIn** | Created Kafka + Avro/Protobuf | Pioneered the schema-first event streaming pattern |

---

## What you'll learn in the next chapters

- **[Chapter 9: Proto Schema](./proto-schema.md)** — how to write `.proto` files
- **[Chapter 10: Binary Encoding](./binary-encoding.md)** — exactly how data becomes bytes, byte by byte
- **[Chapter 11: Compile Workflow](./compile-workflow.md)** — how `protoc` turns `.proto` into Python
- **[Chapter 12: Python Usage](./python-usage.md)** — using the generated classes in practice

---

> ← [Previous: Rebalancing](../kafka/rebalancing.md) | [Index](../README.md) | [Next: Proto Schema →](./proto-schema.md)
