# Schema Evolution

> **You are here:** [Index](../README.md) → [Schema Registry](./README.md) → **Schema Evolution**

---

## The question

You add a new field to `order.proto`. Which schema does the consumer now use?

The answer depends on **when the message was produced**, not when the consumer reads it.

---

## Each message carries its own schema ID

Because of the [5-byte prefix](./wire-format.md), every message on the topic is self-describing. The schema ID baked into the message tells the consumer exactly which version of the schema was used to write it.

```
old message  →  [00][00 00 00 01][00][ protobuf bytes ]   ← schema ID = 1
new message  →  [00][00 00 00 02][00][ protobuf bytes ]   ← schema ID = 2
```

Both messages can coexist on the same topic indefinitely. The consumer handles each correctly by fetching the right schema for each ID.

---

## A concrete walkthrough

**Starting state**

Schema v1 is registered with ID = 1:
```protobuf
message Order {
  string id     = 1;
  string item   = 2;
  double amount = 3;
}
```

**You add a field**

You update `order.proto`:
```protobuf
message Order {
  string id      = 1;
  string item    = 2;
  double amount  = 3;
  string region  = 4;   // new field
}
```

**The steps that actually need to happen**

1. Recompile: `protoc` → regenerate `order_pb2.py`
2. Redeploy the producer — on first produce, `ProtobufSerializer` posts the new schema to the registry, gets back ID = 2
3. Producer now writes messages tagged with ID = 2
4. Consumers automatically pick up schema ID = 2 from the registry on the first message that carries it

The consumer does not need to be redeployed for deserialization to work. It fetches the new schema on demand. However, your consumer code (`consumer.py`) won't see `order.region` until it's updated to reference the new field.

---

## What consumers see during the transition

During a rolling deploy, both old and new messages are on the topic simultaneously:

| Message age | Schema ID | `order.region` in consumer |
|-------------|-----------|---------------------------|
| Old (before deploy) | 1 | `""` — field missing, defaults to empty string |
| New (after deploy) | 2 | actual value, e.g. `"eu-west"` |

There is no error, no crash — Protobuf fills in defaults for missing fields. This is [backward compatibility](./compatibility-modes.md) at the wire level.

---

## Safe changes vs breaking changes

| Change | Safe? | What happens |
|--------|-------|-------------|
| Add field with new field number | ✅ | Old messages: field missing → default value. Old consumers: unknown field → silently ignored |
| Remove a field | ✅ wire / ⚠️ logic | Old messages: extra bytes → ignored. Your code loses access to that data |
| Rename a field | ✅ | Field numbers unchanged, only the Python attribute name changes |
| Change field type (same number) | ❌ | Bytes decoded as wrong type — parse error or silent garbage |
| Reuse a field number for a new field | ❌ | Old messages decoded as wrong meaning — silent data corruption |
| Change field number | ❌ | Same as above — this is the [schema disaster](../protobuf/schema-story.md) scenario |

**The golden rule: field numbers are permanent. Never reuse or change them.**

---

## What the registry checks

Without a compatibility mode set, the registry accepts any schema — including breaking ones. It stores the new schema, assigns it an ID, and lets the producer use it. The damage happens at consume time.

With `FULL` compatibility mode set, the registry rejects the registration if the new schema is not safe in both directions. The producer fails to start before any bad message is written. See [Compatibility Modes](./compatibility-modes.md).

---

## Redpanda Console

After adding a field, update the proto file in `/proto` (mounted into the Console container). Console's [filesystem-based proto decoding](../infra/redpanda-console.md) picks up the change immediately — no restart needed. Old messages still decode correctly because protobuf ignores unknown fields from the consumer's perspective.

---

> ← [Previous: Python Integration](./python-integration.md) | [Part 4 Index](./README.md) | [Next: Schema Caching →](./schema-caching.md)
