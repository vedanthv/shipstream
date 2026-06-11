# Chapter 8 — What is Protobuf?

> **You are here:** [Index](../README.md) → [Rebalancing](../kafka/rebalancing.md) → **What is Protobuf?**

---

## The serialization problem

Services exchange data over a network. Networks transmit bytes. Your Python objects are not bytes. You need a way to convert between them — this is **serialization** (object → bytes) and **deserialization** (bytes → object).

The simplest approach is JSON. But JSON has problems at scale:

```json
{
  "id": "abc-123",
  "customer_id": "customer-42",
  "item": "Mechanical Keyboard",
  "amount": 149.99,
  "status": "ORDER_STATUS_CREATED",
  "created_at": "2024-01-15T10:30:00Z"
}
```

Every message carries the field names. `"customer_id"` is 13 characters every single time. With millions of messages per minute, that's hundreds of megabytes of field names — pure overhead.

---

## What Protobuf does differently

Protocol Buffers (Protobuf) replaces field names with **numbers**. Instead of sending `"customer_id": "c-42"`, it sends `field 2: "c-42"`. The name never travels on the wire.

```
JSON (~90 bytes):
{"id":"abc","customer_id":"c-42","item":"Keyboard","amount":149.99,...}

Protobuf (~25 bytes):
[field 1: "abc"][field 2: "c-42"][field 3: "Keyboard"][field 4: 149.99]...
as binary: 0x0a 03 61 62 63 12 04 63 2d 34 32 ...
```

Both sides know the schema. The schema says field 2 is `customer_id`. So the receiver reads "field 2" in the binary and knows it's `customer_id` without the name ever being sent.

---

## JSON vs Protobuf

| | JSON | Protobuf |
|--|------|----------|
| Format | Human-readable text | Binary bytes |
| Size | ~90 bytes for an Order | ~25 bytes for the same Order |
| Schema | Optional — any field can appear | Required — both sides must agree |
| Speed | Slower parse (text parsing) | Faster parse (binary, fixed positions) |
| Versioning | Manual — no built-in compatibility | Field numbers give backwards compatibility |
| Debugging | Easy — readable in any text editor | Hard — unreadable without the schema |

For Kafka at high throughput, the size difference matters enormously. 3x smaller messages = 3x more throughput on the same hardware, 3x less storage, 3x less network bandwidth.

---

## Industry adoption

| Company | Why Protobuf over JSON |
|---------|----------------------|
| **Google** | Created it. Uses it for virtually all internal RPC |
| **Uber** | Billions of GPS pings/day — JSON would be prohibitively large |
| **Netflix** | Enforces schema contracts between 1000+ microservices |
| **Square** | Payment data must have strict types — no accidental string amounts |
| **Dropbox** | Storage metadata — compact encoding saves real money at scale |

---

## The core concept: field numbers

This is the single most important thing to understand about Protobuf:

```protobuf
message Order {
  string id          = 1;   // ← "1" is what goes on the wire
  string customer_id = 2;   // ← "2" is what goes on the wire
  string item        = 3;
  double amount      = 4;
  OrderStatus status = 5;
  Timestamp created_at = 6;
}
```

The number after `=` is the **field number**. It identifies the field on the wire. The name (`customer_id`) is only used in your code — it never leaves the machine.

**This means:**
- You can rename `customer_id` to `buyer_id` freely — the wire format doesn't change
- You can add new fields (new numbers) and old consumers will ignore them gracefully
- You can remove fields — old producers that still send them will be ignored by new consumers
- You **cannot** change a field number — that would silently corrupt data for any consumer that doesn't re-deploy simultaneously

---

> ← [Previous: Rebalancing](../kafka/rebalancing.md) | [Index](../README.md) | [Next: Proto Schema →](./proto-schema.md)
