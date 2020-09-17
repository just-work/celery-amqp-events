from functools import wraps

from celery.canvas import Signature

from amqp_events import tasks
from amqp_events.celery import celery


def event(name):
    def inner(func):
        @wraps(func)
        def send_event(*args, **kwargs):
            # signature check
            func(*args, **kwargs)
            # send event
            s = Signature(
                args=args,
                kwargs=kwargs,
                task=name,
                app=celery,
                task_type=celery.Task)
            s.apply_async()

        return send_event

    return inner


def handler(name):
    return celery.task(name=name, base=tasks.EventHandler)

