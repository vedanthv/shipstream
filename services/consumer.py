import sys
import os
sys.path.insert(0, '/home/ubuntu/shipstream')

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from generated.order.v1.order_pb2 import Order, OrderStatus

BROKER          = "localhost:19092"
SCHEMA_REGISTRY = "http://localhost:18081"
TOPIC           = "order.created"
GROUP_ID        = "shipstream-consumer-group"
CONSUMER_ID     = os.environ.get("CONSUMER_ID", "1")

STATUS_NAMES = {v: k for k, v in OrderStatus.items()}

TAG = f"[Consumer-{CONSUMER_ID}]"


def main():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    protobuf_deserializer = ProtobufDeserializer(Order, schema_registry_client=schema_registry_client)

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "client.id": f"shipstream-consumer-{CONSUMER_ID}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    print(f"{TAG} Listening on '{TOPIC}' (group: {GROUP_ID}) — Ctrl+C to stop\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"{TAG} [EOF] partition {msg.partition()}")
                else:
                    print(f"{TAG} [ERROR] {msg.error()}")
                continue

            raw = msg.value()
            if raw is None:
                print(f"{TAG} [WARN] None value, skipping")
                continue

            try:
                order = protobuf_deserializer(raw, SerializationContext(TOPIC, MessageField.VALUE))
            except Exception as e:
                print(f"{TAG} [ERROR] Failed to deserialize: {e}")
                continue

            region = f" region={order.region}" if order.region else ""
            print(f"{TAG} partition={msg.partition()} offset={msg.offset()} | "
                  f"id={order.id[:8]}... customer={order.customer_id} "
                  f"item='{order.item}' amount=${order.amount:.2f} "
                  f"status={STATUS_NAMES.get(order.status, 'UNKNOWN')}{region}")

    except KeyboardInterrupt:
        print(f"\n{TAG} Shutting down.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
