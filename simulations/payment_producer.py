"""
Payment producer — publishes Payment messages to payment.processed.

Used as the data source for Phase 3 simulations.

Usage:
    python3 simulations/payment_producer.py              # 200 payments, default topic
    python3 simulations/payment_producer.py --topic payment.procesed --count 50
    python3 simulations/payment_producer.py --silent     # no per-message output
"""
import sys
import uuid
import time
import random
import argparse
sys.path.insert(0, '/home/ubuntu/shipstream')

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from google.protobuf.timestamp_pb2 import Timestamp
from generated_proto_objects.payment.v1.payment_pb2 import Payment, PaymentStatus

BROKER  = "localhost:19092"
SR_URL  = "http://localhost:18081"

PROCESSORS = ["stripe", "adyen", "braintree", "checkout.com"]
CURRENCIES = ["USD", "EUR", "GBP", "SGD"]
STATUSES   = [
    PaymentStatus.PAYMENT_STATUS_PENDING,
    PaymentStatus.PAYMENT_STATUS_AUTHORIZED,
    PaymentStatus.PAYMENT_STATUS_CAPTURED,
    PaymentStatus.PAYMENT_STATUS_FAILED,
]


def make_payment():
    ts = Timestamp()
    ts.FromMilliseconds(int(time.time() * 1000))
    return Payment(
        payment_id=str(uuid.uuid4()),
        order_id=str(uuid.uuid4()),
        customer_id=f"cust-{random.randint(1, 200)}",
        amount=round(random.uniform(5.00, 2000.00), 2),
        currency=random.choice(CURRENCIES),
        status=random.choice(STATUSES),
        processor=random.choice(PROCESSORS),
        created_at=ts,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic",  default="payment.processed")
    parser.add_argument("--count",  type=int, default=200)
    parser.add_argument("--silent", action="store_true")
    args = parser.parse_args()

    sr = SchemaRegistryClient({"url": SR_URL})
    serializer = ProtobufSerializer(Payment, sr)
    producer = Producer({"bootstrap.servers": BROKER})

    print(f"Publishing {args.count} payments to '{args.topic}'...")

    delivered = 0

    def on_delivery(err, msg):
        nonlocal delivered
        if err:
            print(f"[ERROR] {err}")
        else:
            delivered += 1
            if not args.silent:
                print(f"[OK] partition={msg.partition()} offset={msg.offset()}")

    for i in range(args.count):
        payment = make_payment()
        producer.produce(
            topic=args.topic,
            key=payment.payment_id.encode(),
            value=serializer(payment, SerializationContext(args.topic, MessageField.VALUE)),
            callback=on_delivery,
        )
        if i % 20 == 0:
            producer.poll(0)

    producer.flush()
    print(f"\nDone — {delivered}/{args.count} payments delivered to '{args.topic}'.")


if __name__ == "__main__":
    main()
