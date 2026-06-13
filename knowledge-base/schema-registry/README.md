# Part 4 — Schema Registry

[← Back to Index](../README.md)

---

| Chapter | Topic | What you'll learn |
|---------|-------|------------------|
| 17 | [What is Schema Registry?](./what-is-schema-registry.md) | Why it exists, what problem it solves, how Redpanda exposes it |
| 18 | [Wire Format](./wire-format.md) | The 5-byte prefix, magic byte, schema ID, how consumers use it |
| 19 | [Compatibility Modes](./compatibility-modes.md) | BACKWARD, FORWARD, FULL — what each allows and forbids, rolling deploy examples |
| 20 | [Python Integration](./python-integration.md) | `ProtobufSerializer`, `ProtobufDeserializer`, schema registration |
| 21 | [Schema Evolution](./schema-evolution.md) | Which schema consumers use after a change, safe vs breaking changes, deploy steps |
| 22 | [Schema Caching](./schema-caching.md) | How `SchemaRegistryClient` caches schemas, one fetch per ID, restart behaviour |

---

**[→ Start with Chapter 17: What is Schema Registry?](./what-is-schema-registry.md)**

## Mental models

1. **Schema Registry makes messages self-describing.** Each message carries a schema ID; consumers look up the schema to decode it correctly.
2. **The 5-byte prefix is not Protobuf.** It is a Confluent envelope (`0x00` + 4-byte ID) prepended before the Protobuf bytes. A raw Protobuf parser will reject it.
3. **Compatibility is enforced on register, not on produce.** The registry rejects a new schema that violates the compatibility mode — before any message is written.
4. **Schema IDs are per-subject, not global.** The subject is typically `<topic>-value`. Different topics have independent schema lineages.
5. **Redpanda ships Schema Registry built-in.** No extra service needed — it is available on `:18081` (external) out of the box.
6. **Each message decodes with the schema used to write it.** Old messages on the topic always decode correctly, even after a schema upgrade — the ID in the prefix locks in the exact schema version.
7. **Schema caching is unbounded and safe.** IDs are immutable, so a cached schema is never stale. One HTTP fetch per unique schema ID, per process lifetime.
