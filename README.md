# Module 5 Assignment: Asynchronous Messaging

This lab demonstrates a producer and two independent worker processes communicating through a RabbitMQ queue. The workers consume messages from the same queue, acknowledge messages after processing, and use a shared file to handle duplicated events.

## Architecture

```text
                         +----------------+
                         | Order Producer |
                         +-------+--------+
                                 |
                                 | publish JSON orders
                                 v
                         +-------+--------+
                         | RabbitMQ      |
                         | queue: orders |
                         +---+--------+--+
                             |        |
                    message  |        | message
                             v        v
                     +-------+--+  +--+--------+
                     | Worker 1 |  | Worker 2 |
                     +----------+  +-----------+
                         competing consumers
```

The workers act as competing consumers. During normal processing, each message is delivered to one worker. If a worker stops before sending its ACK, RabbitMQ can redeliver that message to another worker.

Each order message contains:
- `event_id`: identifies the event and is used for duplicate detection.
- `order_id`: identifies the order.
- `item_id`: identifies the item.
- `quantity`: specifies how many units were ordered.

## Requirements

- Python 3.9 or later
- Docker and Docker Compose
- macOS or Linux because the worker uses `fcntl` for file locking.

## Setup

Open a terminal in the lab directory and run:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Start Docker Desktop if using it, then start RabbitMQ:

```bash
docker compose up -d
docker compose ps
```
Wait until you can log in to the RabbitMQ dashboard before starting the workers. A running container may still be initializing RabbitMQ.
The RabbitMQ dashboard is available at http://localhost:15672. Log in with username `guest` and password `guest`.

## Run the Producer and Workers

Open three terminal tabs in the lab directory. Activate the virtual environment separately in each tab.

Terminal 1:

```bash
source .venv/bin/activate
python worker.py --name worker-1
```

Terminal 2:

```bash
source .venv/bin/activate
python worker.py --name worker-2
```

Wait until both workers print that they are waiting for orders.

Terminal 3:

```bash
source .venv/bin/activate
python producer.py
```

The producer publishes six messages and exits. Both workers remain running and wait for more messages.

The workers simulate business processing by logging the order and waiting two seconds. Each worker accepts at most one unacknowledged message at a time and sends its ACK after handling the message.

## Stop the System

Press Ctrl+C in each running worker terminal, then stop RabbitMQ:

```bash
docker compose stop
```

## Successful Message Processing

Example output from worker 2:

```text
worker-2 RECEIVED event_id=673815ec-127e-4c29-a8ad-536e39c53a4c redelivered=False
worker-2 PROCESSING order_id=order-1 item_id=item-1 quantity=1
worker-2 FINISHED event_id=673815ec-127e-4c29-a8ad-536e39c53a4c; completion recorded
worker-2 ACK SENT event_id=673815ec-127e-4c29-a8ad-536e39c53a4c
```

The worker finishes processing and records completion before sending the ACK. After all six orders were acknowledged, the dashboard showed Ready = 0, Unacked = 0, and Total = 0.

## Competing Consumers

During a normal six-message run, both workers were connected to the same queue in separate terminal processes.

| Worker | Orders handled in this batch |
|---|---|
| worker-1 | order-1, order-3, order-5 |
| worker-2 | order-2, order-4, order-6 |

Selected output from worker 1:

```text
worker-1 RECEIVED event_id=b446bfc3-04c7-44c2-b8a0-b858d334117f redelivered=False
worker-1 PROCESSING order_id=order-1 item_id=item-1 quantity=1
worker-1 FINISHED event_id=b446bfc3-04c7-44c2-b8a0-b858d334117f
worker-1 ACK SENT event_id=b446bfc3-04c7-44c2-b8a0-b858d334117f
```

Selected output from worker 2:

```text
worker-2 RECEIVED event_id=5c14f5f4-eabe-4c08-8965-a95a6890b8ae redelivered=False
worker-2 PROCESSING order_id=order-2 item_id=item-2 quantity=2
worker-2 FINISHED event_id=5c14f5f4-eabe-4c08-8965-a95a6890b8ae
worker-2 ACK SENT event_id=5c14f5f4-eabe-4c08-8965-a95a6890b8ae
```

Across the full batch, each event was processed by only one worker. The exact distribution can vary as an equal split is not required.

## Failure and Redelivery Test

Begin with an empty queue and stop any existing workers with Ctrl+C. Keep `processed_events.txt` which stores the completed event IDs.

Start worker 2 normally in one terminal:

```bash
python worker.py --name worker-2
```

Start worker 1 in another terminal with failure simulation enabled:

```bash
python worker.py --name worker-1 --crash-next
```

Wait until both workers are waiting for orders, then publish a batch from a third terminal:

```bash
python producer.py
```

Worker 1 terminates after processing and saving its first event, but before sending the ACK. Worker 2 remains running.

Observed worker 1 output:

```text
worker-1 RECEIVED event_id=4dca1a5e-14ac-4509-ad9d-f6023778c2bb redelivered=False
worker-1 PROCESSING order_id=order-2 item_id=item-2 quantity=2
worker-1 FINISHED event_id=4dca1a5e-14ac-4509-ad9d-f6023778c2bb; completion recorded
worker-1 CRASHING BEFORE ACK event_id=4dca1a5e-14ac-4509-ad9d-f6023778c2bb
```

Observed worker 2 output:

```text
worker-2 RECEIVED event_id=4dca1a5e-14ac-4509-ad9d-f6023778c2bb redelivered=True
worker-2 DUPLICATE event_id=4dca1a5e-14ac-4509-ad9d-f6023778c2bb; business action skipped
worker-2 ACK SENT event_id=4dca1a5e-14ac-4509-ad9d-f6023778c2bb
```

The matching event ID and `redelivered=True` show that RabbitMQ redelivered the unacknowledged message to Worker 2.

Worker 2 also continued processing the remaining orders:

```text
worker-2 RECEIVED event_id=e5f204be-be20-4bce-b489-b91f3699239b redelivered=False
worker-2 PROCESSING order_id=order-3 item_id=item-3 quantity=3
worker-2 FINISHED event_id=e5f204be-be20-4bce-b489-b91f3699239b; completion recorded
worker-2 ACK SENT event_id=e5f204be-be20-4bce-b489-b91f3699239b
```

This shows that RabbitMQ and the surviving worker remained running.

## Duplicate Handling

Both workers use the same `processed_events.txt` file. Before processing an event, a worker checks whether its `event_id` is recorded. For a new event, the worker performs the simulated action, saves the event ID, and then sends the ACK. For an already recorded event, it logs `DUPLICATE`, skips the business action, and sends the ACK.

The redelivery output above demonstrates that worker 2 did not print `PROCESSING` for the duplicate event. An exclusive file lock protects the check, processing, and recording steps from concurrent access. This simple design serializes the
simulated business actions.

The failure simulation occurs after completion is saved. For a real external action such as charging a customer, a crash between the action and saving its ID would require additional coordination to
prevent repetition.

## Reflection

A message can be delivered more than once when a worker finishes or partially finishes its business work but fails before RabbitMQ receives the ACK. RabbitMQ cannot safely assume the work completed, so it makes the unacknowledged message available for redelivery. Idempotent handling records a unique `event_id` and checks it before performing the business action. This allows a redelivered message to be acknowledged without repeating the same action such as charging a customer twice.