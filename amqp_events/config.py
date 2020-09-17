from celery import Celery
from celery.app.utils import Settings
from kombu import Queue, Exchange
from typing import List

from amqp_events import defaults

AMQP_EVENTS_CONFIG = {
    # Connections
    'broker_url': 'amqp://guest:guest@localhost:5672/',
    'result_backend': None,

    # Queues and routing
    'task_queues': [],
    'task_default_exchange': 'events',
    'task_default_exchange_type': 'topic',
    'task_default_queue': 'events',
    'task_default_routing_key': '#',
    'task_routes': ['amqp_events.config:route_for_event'],

    # Robustness
    'task_acks_late': True,
    'task_reject_on_worker_lost': True,

    # Task discovery
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
                "x-message-ttl": defaults.AMQP_EVENTS_ARCHIVED_MESSAGE_TTL,
                "x-max-length": defaults.AMQP_EVENTS_ARCHIVED_QUEUE_LENGTH,
                "x-queue-mode": "lazy"
            })

        archived_queue.declare(channel=channel)
        archived_queue.maybe_bind(channel=channel)


def get_queues_from_tasks(app: Celery, conf: Settings) -> List[Queue]:
    queues = []
    exchange = Exchange(
        name=conf.task_default_exchange,
        type=conf.task_default_exchange_type)
    for name, task in app.tasks.items():
        if task.__module__ not in conf.imports:
            continue
        queue = Queue(
            name=f'{defaults.AMQP_EVENTS_QUEUE_PREFIX}.{task.name}',
            exchange=exchange,
            routing_key=task.name)
        queues.append(queue)
    return queues


# noinspection PyUnusedLocal
def route_for_event(name, args, kwargs, options, task=None, **kw):
    return {
        'routing_key': options.get('routing_key', name),
        'exchange': task.app.conf.task_default_exchange,
        'exchange_type': task.app.conf.task_default_exchange_type
    }
