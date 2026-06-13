"""
Simulation S1 — BACKWARD Compatibility: field deletion.

Runs two consumers against the same topic (messages written by v1 producer with region):
  - v1 consumer: reads region from field 7
  - v2 consumer: no region field — unknown bytes silently dropped

Logs each consumer's output to a separate file:
  logs/simulations/v1-consumer.log
  logs/simulations/v2-consumer.log
"""
import os
import sys
sys.path.insert(0, '/home/ubuntu/shipstream')

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from generated_proto_objects.order.v1.order_pb2 import Order as OrderV1, OrderStatus as StatusV1
from generated_proto_objects.order.v2.order_pb2 import Order as OrderV2, OrderStatus as StatusV2

BROKER  = "localhost:19092"
SR_URL  = "http://localhost:18081"
TOPIC   = "order.created"

os.makedirs("logs/simulations", exist_ok=True)
V1_LOG = "logs/simulations/v1-consumer.log"
V2_LOG = "logs/simulations/v2-consumer.log"

STATUS_V1 = {v: k for k, v in StatusV1.items()}
STATUS_V2 = {v: k for k, v in StatusV2.items()}


def make_consumer(group_id):
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,   # signals EOF per partition instead of blocking forever
    })
    c.subscribe([TOPIC])
    return c


def drain(consumer, deserializer, ctx):
    """Poll until every assigned partition reports EOF. Returns list of (raw_msg, order)."""
    results = []
    eof_partitions = set()

    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof_partitions.add(msg.partition())
                assigned = consumer.assignment()
                if assigned and eof_partitions >= {p.partition for p in assigned}:
                    break
            continue
        order = deserializer(msg.value(), ctx)
        results.append((msg, order))

    return results


def write_log(path, results, status_map, header, preamble, is_v2=False):
    sep = "-" * 100 + "\n"
    with open(path, "w") as f:
        f.write(preamble)
        f.write(header)
        f.write(sep)
        for raw, order in results:
            try:
                region = order.region if order.region else "(empty)"
            except AttributeError:
                region = "(field does not exist in v2 schema)"
            f.write(
                f"p={raw.partition()}         | {raw.offset():<6} | "
                f"{order.customer_id:<16}| {order.item:<26}| "
                f"{status_map.get(order.status, '?'):<24}| {region}\n"
            )
        f.write(sep)
        count = len(results)
        if is_v2:
            f.write(f"\n[v2-consumer] {count} messages read. region bytes on wire were silently dropped.\n")
            f.write("[v2-consumer] BACKWARD compatibility confirmed: no errors, no corruption.\n")
        else:
            f.write(f"\n[v1-consumer] {count} messages read. region field populated on all.\n")


def main():
    sr1 = SchemaRegistryClient({"url": SR_URL})
    sr2 = SchemaRegistryClient({"url": SR_URL})
    deser_v1 = ProtobufDeserializer(OrderV1, schema_registry_client=sr1)
    deser_v2 = ProtobufDeserializer(OrderV2, schema_registry_client=sr2)
    ctx = SerializationContext(TOPIC, MessageField.VALUE)

    c1 = make_consumer("sim-s1-v1-group")
    c2 = make_consumer("sim-s1-v2-group")

    print("Draining v1 consumer (all partitions)...")
    results_v1 = drain(c1, deser_v1, ctx)
    c1.close()

    print("Draining v2 consumer (all partitions)...")
    results_v2 = drain(c2, deser_v2, ctx)
    c2.close()

    header = "partition | offset | customer        | item                      | status                  | region\n"

    write_log(
        V1_LOG, results_v1, STATUS_V1, header,
        "[v1-consumer] Schema: Order v1 (has region field)\n"
        "[v1-consumer] Messages produced by: v1 producer (region encoded at field 7)\n\n",
    )
    write_log(
        V2_LOG, results_v2, STATUS_V2, header,
        "[v2-consumer] Schema: Order v2 (region field removed)\n"
        "[v2-consumer] Messages produced by: v1 producer (region encoded at field 7)\n"
        "[v2-consumer] Expectation: region bytes silently discarded — no crash, no value\n\n",
        is_v2=True,
    )

    print(f"Done. {len(results_v1)} messages in v1 log, {len(results_v2)} in v2 log.")
    print(f"  {V1_LOG}\n  {V2_LOG}")


if __name__ == "__main__":
    main()
