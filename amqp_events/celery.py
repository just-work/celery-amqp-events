from typing import Any, Callable, TypeVar, Protocol, Set, cast, Type, Tuple

from celery import Celery, Task
from celery.canvas import Signature
from celery.result import AsyncResult
from kombu import Exchange, Queue

from amqp_events import config, task, defaults

AnyFunc = Callable[..., Any]
F = TypeVar('F')
# noinspection PyTypeChecker
T = TypeVar('T', bound=AnyFunc)


class EventProtocol(Protocol[T]):
    """
    EventProtocol describes a function signature that is decorated with
    `EventsCelery.event` decorator:

    * it has same signature as initial function (T vs EventProtocol.__call__)
    * it also has `name` and `app` attributes
    * it has `handler` decorator which may bind and event handler to an event
    """
    name: str
    app: "EventsCelery"

    __call__: T

    def handler(self, func: F) -> F: ...


class EventsCelery(Celery):
    task_cls = task.EventHandler

    def __init__(self, main: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(main, *args, **kwargs)
        self.on_after_finalize.connect(self._generate_task_queues)
        self.on_after_finalize.connect(self._register_retry_queues)
        self._handlers: Set[str] = set()

    def event(self, name: str) -> Callable[[T], EventProtocol[T]]:
        """
        This decorator marks a callable as an event with name `name`
        :param name: event name (also used as Celery task name)
        :return: decorator that adds `name` attribute and `handler` method to
            an event and also sends a message to the celery broker on decorated
            function call.
        """

        def inner(func: T) -> EventProtocol[T]:
            # noinspection PyTypeChecker
            return cast(EventProtocol[T], Event(self, name, func))

        return inner

    def handler(self, name: str, bind: bool = False) -> Callable[[T], T]:
        """
        Decorates a function or a class as a handler for an event.
        :param name: event name
        :param bind: flag to pass Celery.Task instance to a function
            (see `Celery.task(bind=True)`).
        :return: a decorator with `name` and `bind` values set up.
        """

        def inner(fun_or_cls: T) -> T:
            if name in self._handlers:
                raise RuntimeError("event handler already registered")
            self._handlers.add(name)
            return self._create_task_from_handler(
                fun_or_cls, name=name, bind=bind)

        return inner

    def _create_task_from_handler(self, fun_or_cls: T, *, name: str,
                                  bind: bool) -> T:
        """
        Registers a new task from class or function

        :param fun_or_cls: event handler class or function
        :param name: event name to stick handler to
        :param bind: pass `self` argument to a function
            (see `Celery.task(bind=True)`)
        :returns fun_or_cls

        """
        if isinstance(fun_or_cls, type) and issubclass(fun_or_cls, Task):
            task_cls = self._make_task_cls(fun_or_cls, name)
            self.tasks.register(task_cls)
        else:
            self.task(fun_or_cls, name=name, bind=bind)
        return fun_or_cls

    def _make_task_cls(self, handler: Type[Task], name: str) -> Type[Task]:
        """
        Created a new class for a given class base.

        :param handler: handler class
        :param name: event name
        :return: new task class that inherits `task_class` and `self.Task`
            and has `name` attribute set to event name
        """
        if self.Task in handler.__bases__:
            bases: Tuple[Type[Task], ...] = (handler,)
        else:
            bases = (handler, self.Task)
        attrs = {'name': name}
        new_name = getattr(handler, '__name__')
        # noinspection PyTypeChecker
        return type(new_name, bases, attrs)

    def _generate_task_queues(self, **_: Any) -> None:
        """
        Create a list of queues for celery worker from registered handlers list.
        """
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

    def _register_retry_queues(self, **_: Any) -> None:
        """
        Initializes a set of AMQP primitives to implement broker-based delays.
        """
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
    """ Event function wrapper that sends a message to broker"""

    def __init__(self, app: EventsCelery, name: str, func: Callable) -> None:
        self.app = app
        self.name = name
        self.func = func

    def __call__(self, *args: Any, **kwargs: Any) -> str:
        self.func(*args, **kwargs)
        s = Signature(
            args=args,
            kwargs=kwargs,
            task=self.name,
            app=self.app,
            task_type=self.app.Task,
            routing_key=self.name)
        result: AsyncResult = s.apply_async()
        return result.id

    def handler(self, func: Callable) -> Callable:
        return self.app.handler(self.name)(func)


def events_app(main: str, **kwargs: Any) -> EventsCelery:
    """
    Initializes new EventsCelery application
    :param main: application name used as queue prefix
    :param kwargs: additional celery configuration
    :return: applcation instance
    """
    app = EventsCelery(main)
    app.config_from_object(config.AMQP_EVENTS_CONFIG)
    app.conf.update(kwargs)
    return app
