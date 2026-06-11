import sys
import os
sys.path.insert(0, '/home/ubuntu/shipstream')

from confluent_kafka import Consumer, KafkaError
from generated.order.v1.order_pb2 import Order, OrderStatus

BROKER      = "localhost:19092"
TOPIC       = "order.created"
GROUP_ID    = "shipstream-consumer-group"
CONSUMER_ID = os.environ.get("CONSUMER_ID", "1")

STATUS_NAMES = {v: k for k, v in OrderStatus.items()}

TAG = f"[Consumer-{CONSUMER_ID}]"


def main():
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

            order = Order()
            try:
                order.ParseFromString(raw)
            except Exception as e:
                print(f"{TAG} [ERROR] Failed to deserialize: {e}")
                continue

            print(f"{TAG} partition={msg.partition()} offset={msg.offset()} | "
                  f"id={order.id[:8]}... customer={order.customer_id} "
                  f"item='{order.item}' amount=${order.amount:.2f} "
                  f"status={STATUS_NAMES.get(order.status, 'UNKNOWN')}")

    except KeyboardInterrupt:
        print(f"\n{TAG} Shutting down.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
