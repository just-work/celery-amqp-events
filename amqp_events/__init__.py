from functools import partial

from amqp_events.celery import celery


def handler(func=None, name=None):
    if func is None:
        return partial(handler, name=name)
    return celery.task(func, name=name, routing_key=name)
