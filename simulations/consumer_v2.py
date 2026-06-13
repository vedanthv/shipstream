"""
Simulation consumer using Order v2 schema (region field removed).
Demonstrates BACKWARD compatibility: reads old messages written with v1 (which have
region bytes at field 7). Protobuf silently ignores the unknown field.
"""
import sys
sys.path.insert(0, '/home/ubuntu/shipstream')

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from generated_proto_objects.order.v2.order_pb2 import Order, OrderStatus

BROKER          = "localhost:19092"
SCHEMA_REGISTRY = "http://localhost:18081"
TOPIC           = "order.created"
GROUP_ID        = "sim-v2-consumer-group"

STATUS_NAMES = {v: k for k, v in OrderStatus.items()}


def main():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    protobuf_deserializer = ProtobufDeserializer(Order, schema_registry_client=schema_registry_client)

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    print("[v2-consumer] Using Order v2 schema (no region field)")
    print("[v2-consumer] Reading messages written by v1 producer (which had region)\n")

    read = 0
    try:
        while read < 10:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERROR] {msg.error()}")
                continue

            order = protobuf_deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
            print(f"partition={msg.partition()} offset={msg.offset()} | "
                  f"id={order.id[:8]}... customer={order.customer_id} "
                  f"item='{order.item}' amount=${order.amount:.2f} "
                  f"status={STATUS_NAMES.get(order.status, 'UNKNOWN')}")
            read += 1
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"\n[v2-consumer] Done — {read} messages read. No errors despite region bytes in old messages.")


if __name__ == "__main__":
    main()
