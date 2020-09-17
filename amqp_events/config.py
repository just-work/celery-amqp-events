from celery import Celery
from kombu import Queue
from typing import List

from amqp_events import defaults

AMQP_EVENTS_CONFIG = {
    'broker_url': 'amqp://guest:guest@localhost:5672/',
    'result_backend': None,
    'task_queues': [],
    'task_default_exchange': 'events',
    'task_default_exchange_type': 'topic',
    'task_default_queue': 'events',
    'task_default_routing_key': '#',
    'imports': [],
}


def initialize_task_queues(app: Celery, queues: List[Queue]):
    channel = app.broker_connection().default_channel
    for queue in queues:
        retry_queue = Queue(
            name=f'{queue.name}.retry',
            routing_key=f'{queue.routing_key}.retry',
            exchange=queue.exchange,
            queue_arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue.name
            }
        )

        retry_queue.declare(channel=channel)
        retry_queue.maybe_bind(channel=channel)

        archived_queue = Queue(
            name=f'{queue.name}.archived',
            routing_key=f'{queue.routing_key}.archived',
            exchange=queue.exchange,
            queue_arguments={
                "x-message-ttl": defaults.ARCHIVED_MESSAGE_TTL * 1000,
                "x-max-length": defaults.ARCHIVED_QUEUE_LENGTH,
                "x-queue-mode": "lazy"
            })

        archived_queue.declare(channel=channel)
        archived_queue.maybe_bind(channel=channel)
