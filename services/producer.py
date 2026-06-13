import sys
import uuid
import time
import random
sys.path.insert(0, '/home/ubuntu/shipstream')

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from google.protobuf.timestamp_pb2 import Timestamp
from generated_proto_objects.order.v1.order_pb2 import Order, OrderStatus

BROKER          = "localhost:19092"
SCHEMA_REGISTRY = "http://localhost:18081"
TOPIC           = "order.created"
COUNT           = 100

REGIONS = ["us-east", "us-west", "eu-west", "ap-southeast"]

ITEMS = [
    "Mechanical Keyboard", "USB-C Hub", "Standing Desk", "Monitor Arm",
    "Webcam", "Noise-Cancelling Headphones", "Laptop Stand", "Mouse Pad",
    "Ergonomic Chair", "LED Desk Lamp",
]

STATUSES = [
    OrderStatus.ORDER_STATUS_CREATED,
    OrderStatus.ORDER_STATUS_PAID,
    OrderStatus.ORDER_STATUS_FULFILLED,
]


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] partition={msg.partition()} offset={msg.offset()}")


def main():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    protobuf_serializer = ProtobufSerializer(Order, schema_registry_client)

    producer = Producer({"bootstrap.servers": BROKER})

    print(f"Publishing {COUNT} orders to '{TOPIC}' (with Schema Registry)...\n")

    for i in range(COUNT):
        ts = Timestamp()
        ts.FromMilliseconds(int(time.time() * 1000))

        order = Order(
            id=str(uuid.uuid4()),
            customer_id=f"customer-{random.randint(1, 50)}",
            item=random.choice(ITEMS),
            amount=round(random.uniform(9.99, 499.99), 2),
            status=random.choice(STATUSES),
            created_at=ts,
            region=random.choice(REGIONS),
        )

        producer.produce(
            topic=TOPIC,
            key=order.id.encode(),
            value=protobuf_serializer(order, SerializationContext(TOPIC, MessageField.VALUE)),
            callback=delivery_report,
        )

        # poll periodically to fire delivery callbacks and avoid buffer buildup
        if i % 10 == 0:
            producer.poll(0)

    producer.flush()
    print(f"\nDone — {COUNT} orders published.")


if __name__ == "__main__":
    main()
