""" Default configuration for EventsCelery."""
from typing import Any, Dict, Tuple, Optional

from celery.app.task import Task

AMQP_EVENTS_CONFIG: Dict[str, Any] = {
    # Connections
    'broker_url': 'amqp://guest:guest@localhost:5672/',
    'result_backend': None,

    # Queues and routing
    'task_queues': [],
    'task_default_exchange': 'events',
    'task_default_exchange_type': 'topic',
    'task_default_queue': 'events',
    'task_default_routing_key': 'events',
    'task_routes': ['amqp_events.config:route_for_event'],

    # Robustness
    'task_acks_late': True,
    'task_acks_on_failure_or_timeout': True,
    'task_reject_on_worker_lost': True,
    'broker_transport_options': {'confirm_publish': True},
}


# noinspection PyUnusedLocal
def route_for_event(name: str,
                    args: Tuple[Any, ...],
                    kwargs: Dict[str, Any],
                    options: Dict[str, Any],
                    task: Optional[Task] = None,
                    **kw: Any) -> Dict[str, str]:
    # Without explicit routing function Celery tries to declare and bind
    # default queue while sending events, which leads to unexpected behavior.
    return {
        'routing_key': options.get('routing_key', name),
        # 'exchange': task.app.conf.task_default_exchange,
        # 'exchange_type': task.app.conf.task_default_exchange_type
    }
