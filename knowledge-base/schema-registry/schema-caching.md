# Schema Caching

> **You are here:** [Index](../README.md) → [Schema Registry](./README.md) → **Schema Caching**

---

## The short answer

`SchemaRegistryClient` caches schemas in memory after the first fetch. For a given schema ID, only **one HTTP call** is ever made per process lifetime.

---

## How the cache works

Every message the consumer receives starts with a schema ID in bytes 1–4. Before decoding, `ProtobufDeserializer` checks a local dict:

```
message arrives → extract schema ID (e.g. 3)
                       │
                       ▼
              ID 3 in local cache?
              YES ──→ use cached schema  (no network call)
              NO  ──→ GET /schemas/ids/3 from registry
                          │
                          ▼
                      cache it → use it
```

In practice, a consumer processing thousands of messages per second only makes one or two HTTP calls to the registry at startup (one per unique schema ID it encounters).

---

## What the cache is

The cache is a plain in-memory dict inside the `SchemaRegistryClient` instance. It maps schema ID → parsed schema object.

```python
# conceptually, inside SchemaRegistryClient
self._cache = {
    1: <Schema for Order v1>,
    2: <Schema for Order v2>,
}
```

It is not persisted to disk, not shared between processes, and not invalidated on any schedule.

---

## Scope and lifetime

| Property | Value |
|----------|-------|
| Scope | One Python process |
| Lifetime | Until the process exits |
| Shared across threads? | Yes — the client is thread-safe |
| Shared across processes? | No — each consumer process has its own cache |
| Persisted between restarts? | No — cache is rebuilt from scratch on each restart |

If you run 3 consumer processes, each one independently caches schemas as it encounters them.

---

## Why this is safe

Schema IDs are **immutable**. Once the registry assigns ID `3` to a schema, that mapping never changes. There is no scenario where the registry returns a different schema for the same ID later. This guarantee is what makes an unbounded, never-invalidated cache correct.

If you register a new schema version, it gets a new ID (e.g. `4`). Old messages still carry ID `3` in their prefix and are still decoded with schema `3`. New messages carry ID `4` and trigger a fresh fetch on first encounter.

```
schema v1 registered → ID = 1  (permanent, never changes)
schema v2 registered → ID = 2  (permanent, never changes)

consumer cache after processing both:
  { 1: Schema(v1), 2: Schema(v2) }
```

---

## What happens on consumer restart

The cache is empty. On the first message with each schema ID, the consumer makes a network call to the registry. If the registry is unreachable at that moment, deserialization fails with a connection error.

This is why the registry should be treated as an availability dependency — like the broker itself. If it's down, new consumer processes can't decode messages until it comes back.

---

## Verifying the registry is being hit

You can confirm the first-fetch behavior by watching registry logs or using `curl` to observe:

```bash
# Check all registered schemas (hit this before and after starting a consumer)
curl http://localhost:18081/subjects

# Fetch a specific schema by ID — same call the consumer makes internally
curl http://localhost:18081/schemas/ids/1
```

The consumer makes exactly that GET request once per unique schema ID, then never again for that process.

---

> ← [Previous: Schema Evolution](./schema-evolution.md) | [Part 4 Index](./README.md) | [Back to Main Index →](../README.md)
