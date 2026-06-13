# Compatibility Modes

> **You are here:** [Index](../README.md) → [Schema Registry](./README.md) → **Compatibility Modes**

---

## What compatibility means

Schema Registry enforces compatibility rules when you try to register a new schema version. If the new schema violates the current compatibility mode, the registration is **rejected** — before any producer can use it, before any bytes land in Kafka. This is the gate that prevents the [schema disaster](../protobuf/schema-story.md) at the source.

The compatibility check answers: *can consumers or producers using the old schema still work correctly when someone deploys the new schema?*

---

## The four modes

### BACKWARD (default)

New consumers (with the new schema) can read messages written by old producers (with the old schema).

**Allowed changes:**
- Add a new optional field
- Delete an existing field

**Forbidden changes:**
- Rename a field
- Change a field's type
- Change a field's number (this is the disaster scenario)

**Why it's the default:** You typically deploy consumers before producers. BACKWARD ensures the new consumer can process all existing messages in the topic before the new producer starts writing new ones.

```
Old producer writes:  [id, customer_id, item, amount, status, created_at]
New consumer reads:   [id, customer_id, item, amount, status, created_at, region]
                                                                          ↑ missing → empty string, OK
```

### FORWARD

Old consumers (with the old schema) can read messages written by new producers (with the new schema).

**Allowed changes:**
- Add a new optional field
- Delete an existing field

**When to use it:** You deploy producers before consumers. FORWARD ensures the old consumer can still process messages produced with the new schema.

```
New producer writes:  [id, customer_id, item, amount, status, created_at, region]
Old consumer reads:   [id, customer_id, item, amount, status, created_at]
                                                                          ↑ unknown field → ignored, OK
```

### FULL

Both BACKWARD and FORWARD simultaneously. The new schema must be readable by old consumers and the new consumer must be able to read old messages.

**Allowed changes:**
- Add a new optional field (this is the only safe change for FULL)

**When to use it:** When you can't control deployment order and need to guarantee zero breakage regardless.

### NONE

No compatibility checks. Any schema is accepted. Useful during development but dangerous in production.

---

## The additive-only rule

For Protobuf, the only change that satisfies **both** BACKWARD and FORWARD (i.e., FULL) is adding a new optional field. Everything else either breaks one or both directions.

| Change | BACKWARD | FORWARD |
|--------|----------|---------|
| Add optional field | ✅ | ✅ |
| Delete field | ✅ | ❌ |
| Change field number | ❌ | ❌ |
| Change field type | ❌ | ❌ |
| Rename field | ✅* | ✅* |

*Renaming is safe in Protobuf because the wire format uses field numbers, not names. The new `_pb2.py` uses a different Python attribute name, but the bytes are identical.

---

## How Redpanda enforces it

When you register a new schema version, Redpanda runs the compatibility check against the existing versions for that subject before persisting anything.

```
POST /subjects/order.created-value/versions
  { "schema": "... new proto definition ...", "schemaType": "PROTOBUF" }

→ 200 OK: { "id": 2 }       ← compatible, registered
→ 409 Conflict: { "error_code": 409, "message": "Schema being registered is incompatible..." }
```

If the registration fails, no producer can use the new schema — the problem is caught before it ever reaches Kafka.

---

## Detailed examples with a rolling deploy story

These examples use a concrete scenario: you have a producer and two consumer versions in a rolling deploy.

**Schema v1 (currently in production)**
```protobuf
message Order {
  string id     = 1;
  string item   = 2;
  double amount = 3;
}
```

**Schema v2 (the change you want to deploy)**
```protobuf
message Order {
  string id      = 1;
  string item    = 2;
  double amount  = 3;
  string region  = 4;   // new field
}
```

---

### BACKWARD — "new consumer, old messages"

The question: can **Consumer v2** (knows schema v2) read messages written by **Producer v1** (used schema v1)?

**Example 1 — Adding a field ✅**

Producer v1 wrote: `id="abc", item="shoes", amount=99.0` — no `region` bytes in the message.

Consumer v2 reads it:
```
id = "abc", item = "shoes", amount = 99.0, region = ""
                                                    ↑ field missing → Protobuf defaults to ""
```
Registry **accepts** the schema change. ✅

**Example 2 — Changing a field type ❌**

Schema v2 changes `item` from `string` to `int32` (same field number 2):
```protobuf
int32 item = 2;   // was string
```

Producer v1 wrote `item` as a length-prefixed string. Consumer v2 tries to decode field `2` as a 32-bit integer. The bytes are in the wrong format — **parse error, consumer crashes.**

Registry **rejects** this schema. ❌

---

### FORWARD — "old consumer, new messages"

The question: can **Consumer v1** (knows schema v1) read messages written by **Producer v2** (used schema v2)?

This matters when the producer is deployed first and old consumer instances are still running.

**Example 1 — Adding a field ✅**

Producer v2 writes: `id="abc", item="shoes", amount=99.0, region="eu-west"` — field `4` bytes are in the message.

Consumer v1 reads it:
```
id = "abc", item = "shoes", amount = 99.0, [field 4 unknown → silently ignored]
```
Protobuf ignores unknown fields. Consumer v1 works fine. Registry **accepts** it. ✅

**Example 2 — Changing a field type ❌**

Same scenario as above — Producer v2 encodes field `2` as `int32`, Consumer v1 expects field `2` to be a string. **Consumer v1 crashes.**

Registry **rejects** this schema. ❌

---

### FULL — both at the same time

Every registered schema must pass both BACKWARD and FORWARD checks. The only change that always satisfies both:

> **Add a new optional field with a new field number.**

Everything else fails one or both checks:

| Change | BACKWARD | FORWARD | FULL |
|--------|----------|---------|------|
| Add optional field (new number) | ✅ | ✅ | ✅ |
| Delete existing field | ✅ | ✅* | ✅* |
| Change field type | ❌ | ❌ | ❌ |
| Change/reuse field number | ❌ | ❌ | ❌ |

*Deleting a field technically passes both wire-format checks in Protobuf — missing fields default, unknown fields are ignored. But your application code loses access to that data. The registry doesn't know about application-level semantics.

---

### NONE — no checks

Any schema is accepted, including ones that break consumers. The registry stores it, assigns an ID, and lets the producer use it. Damage happens silently at consume time.

Fine for local development. Never use in production.

---

## Which mode to use

| Mode | Protects against | Use when |
|------|-----------------|----------|
| `NONE` | Nothing | Local dev / learning |
| `BACKWARD` | New consumer can't read old messages | You always deploy consumers before producers |
| `FORWARD` | Old consumer can't read new messages | You always deploy producers before consumers |
| `FULL` | Both | Production — can't control deploy order |

---

## Setting the compatibility mode

```bash
# Check current mode for a subject
curl http://localhost:18081/config/order.created-value

# Set mode for a subject
curl -X PUT http://localhost:18081/config/order.created-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FULL"}'

# Set global default
curl -X PUT http://localhost:18081/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

---

> ← [Previous: Wire Format](./wire-format.md) | [Part 4 Index](./README.md) | [Next: Python Integration →](./python-integration.md)
