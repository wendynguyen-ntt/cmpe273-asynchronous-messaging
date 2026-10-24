import json
import uuid

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()
channel.queue_declare(queue="orders", durable=True)

for number in range (1,7):
    message = {
        "event_id": str(uuid.uuid4()),
        "order_id": f"order-{number}",
        "item_id": f"item-{number}",
        "quantity": number,
    }

    channel.basic_publish(
        exchange="",
        routing_key="orders",
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )

    print(f"PUBLISHED: {message}")

connection.close()
print("Finished publishing 6 order messages.")