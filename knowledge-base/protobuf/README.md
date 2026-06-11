# Part 2 — Protobuf

[← Back to Index](../README.md)

---

| Chapter | Topic | What you'll learn |
|---------|-------|------------------|
| 8 | [What is Protobuf?](./what-is-protobuf.md) | Binary serialization, JSON vs Protobuf, field numbers, industry adoption |
| 9 | [Proto Schema](./proto-schema.md) | `.proto` syntax, wire types, tag formula, field type table, schema evolution |
| 10 | [Binary Encoding](./binary-encoding.md) | Byte-by-byte teardown of a real message, varints, IEEE 754, tag decoding |
| 11 | [Compile Workflow](./compile-workflow.md) | `protoc` internals, file descriptor, generated code structure, `__init__.py` |
| 12 | [Python Usage](./python-usage.md) | Constructing, serializing, deserializing, introspection, error handling |

---

**[→ Start with Chapter 8: What is Protobuf?](./what-is-protobuf.md)**

## Mental models

1. **Field numbers are the contract, not names.** Rename fields freely — never change their numbers.
2. **`order_pb2.py` is a build artifact.** Always edit `.proto`, then regenerate. Never edit the generated file.
3. **Bytes on the wire contain no field names.** Only field numbers, wire types, and values.
4. **Zero values are invisible.** Unset fields aren't written to the wire — set your zero enum value to `UNSPECIFIED`.
5. **The schema IS Protobuf.** The file descriptor blob in `_pb2.py` is itself a serialized Protobuf message.
