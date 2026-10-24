import argparse
import json
import time
import os
import fcntl
from pathlib import Path

import pika

PROCESSED_FILE = Path(__file__).with_name("processed_events.txt")

parser = argparse.ArgumentParser()
parser.add_argument("--name", required=True)
parser.add_argument("--crash-next", action="store_true")
args = parser.parse_args()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()
channel.queue_declare(queue="orders", durable=True)
channel.basic_qos(prefetch_count=1)

def process_order(ch, method, properties, body):
    message = json.loads(body)
    event_id = message["event_id"]

    print(
        f"{args.name} RECEIVED event_id={event_id} "
        f"redelivered={method.redelivered}",
        flush=True,
    )

    with PROCESSED_FILE.open("a+", encoding="utf-8") as ledger:
        fcntl.flock(ledger.fileno(), fcntl.LOCK_EX)

        ledger.seek(0)
        processed_ids = set(ledger.read().splitlines())

        if event_id in processed_ids:
            print(
                f"{args.name} DUPLICATE event_id={event_id}; "
                "business action skipped",
                flush=True,
            )
        else:
            print(
                f"{args.name} PROCESSING order_id={message['order_id']} "
                f"item_id={message['item_id']} quantity={message['quantity']}",
                flush=True,
            )

            time.sleep(2)

            ledger.write(event_id + "\n")
            ledger.flush()
            os.fsync(ledger.fileno())

            print(
                f"{args.name} FINISHED event_id={event_id}; "
                "completion recorded",
                flush=True,
            )

    if args.crash_next:
        print(
            f"{args.name} CRASHING BEFORE ACK event_id={event_id}",
            flush=True,
        )
        os._exit(1)

    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(
        f"{args.name} ACK SENT event_id={event_id}",
        flush=True,
    )

channel.basic_consume(
    queue="orders",
    on_message_callback=process_order,
    auto_ack=False,
)

print(f"{args.name} waiting for orders. Press Ctrl+C to stop.", flush=True)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
finally:
    connection.close()