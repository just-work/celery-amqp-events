from typing import Callable

from celery import Celery
from celery.canvas import Signature
from celery.result import AsyncResult

from amqp_events import tasks


class EventsCelery(Celery):
    def event(self, name: str) -> Callable[[Callable], "Event"]:
        def inner(func):
            return Event(self, name, func)

        return inner

    def handler(self, name: str) -> Callable[[Callable], Callable]:
        return self.task(name=name, base=tasks.EventHandler)


class Event:
    def __init__(self, app: EventsCelery, name: str, func: Callable) -> None:
        self.app = app
        self.name = name
        self.func = func

    def __call__(self, *args, **kwargs) -> str:
        self.func(*args, **kwargs)
        s = self.make_signature(args, kwargs)
        result: AsyncResult = s.apply_async()
        return result.id

    def handler(self, func: Callable) -> Callable:
        return self.app.handler(self.name)(func)

    def make_signature(self, args, kwargs):
        return Signature(
            args=args,
            kwargs=kwargs,
            task=self.name,
            app=self.app,
            task_type=self.app.Task)
