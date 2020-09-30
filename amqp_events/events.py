from collections import defaultdict
from functools import partial
from typing import Callable

from celery import Celery, Task
from celery.canvas import Signature
from celery.result import AsyncResult
from kombu import Exchange, Queue

from amqp_events import tasks, defaults


class EventsCelery(Celery):
    task_cls = tasks.EventHandler

    def __init__(self, main, *args, **kwargs):
        super().__init__(main, *args, **kwargs)
        self.on_after_finalize.connect(self._generate_task_queues)
        self.on_after_finalize.connect(self._register_retry_queues)
        self._handlers = set()

    def event(self, name: str) -> Callable[[Callable], "Event"]:
        def inner(func):
            return Event(self, name, func)

        return inner

    def handler(self, name: str, bind: bool = False):
        if name in self._handlers:
            raise RuntimeError("event handler already registered")
        self._handlers.add(name)
        return partial(self._create_task_from_handler, name=name, bind=bind)

    def _create_task_from_handler(self, fun_or_cls, *, name, bind):
        if isinstance(fun_or_cls, type) and issubclass(fun_or_cls, Task):
            if self.Task in fun_or_cls.__bases__:
                bases = (fun_or_cls,)
            else:
                bases = (fun_or_cls, self.Task)
            attrs = {'name': name}
            new_name = getattr(fun_or_cls, '__name__')
            task_class = type(new_name, bases, attrs)
            self.register_task(task_class)
        else:
            self.task(fun_or_cls, name=name, bind=bind)
        return fun_or_cls

    def _generate_task_queues(self, **_):
        queues = self.conf.task_queues or []
        if queues:
            return
        exchange = Exchange(
            name=self.conf.task_default_exchange,
            type=self.conf.task_default_exchange_type)
        for name in self._handlers:
            queue = Queue(
                name=f'{self.main}.{name}',
                exchange=exchange,
                routing_key=name)
            queues.append(queue)
        self.conf.task_queues = queues

    def _register_retry_queues(self, **_):
        channel = self.broker_connection().default_channel
        for queue in self.conf.task_queues:
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
            task_type=self.app.Task,
            routing_key=self.name)
