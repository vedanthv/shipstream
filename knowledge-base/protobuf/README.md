# Part 2 — Protobuf

[← Back to Index](../README.md)

---

| Chapter | Topic | One-liner |
|---------|-------|-----------|
| 8 | [What is Protobuf?](./what-is-protobuf.md) | Binary serialization, why not JSON, field numbers |
| 9 | [Proto Schema](./proto-schema.md) | `.proto` syntax, messages, enums, well-known types |
| 10 | [Compile Workflow](./compile-workflow.md) | `protoc`, generated files, `__init__.py` |
| 11 | [Python Usage](./python-usage.md) | Constructing, serializing, deserializing |

---

**[→ Start with Chapter 8: What is Protobuf?](./what-is-protobuf.md)**

## Mental models

1. **Field numbers are the contract, not names.** Rename fields freely — never change their numbers.
2. **`order_pb2.py` is a build artifact.** Always edit `.proto`, then regenerate. Never edit the generated file.
3. **Bytes on the wire contain no field names.** Only numbers and values — that's why it's compact.
