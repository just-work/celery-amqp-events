from typing import Any, Callable, TypeVar, Protocol, Set, cast, Type, Tuple, \
    overload
from typing import Optional, Union

from celery.app.base import Celery
from celery.app.task import Task
from celery import canvas
from celery.result import AsyncResult
from kombu import Exchange, Queue, binding

from amqp_events import config, task, defaults

# queue arguments
X_QUEUE_MODE = "x-queue-mode"
X_MAX_LENGTH = "x-max-length"
X_MESSAGE_TTL = "x-message-ttl"
X_DEAD_LETTER_EXCHANGE = "x-dead-letter-exchange"

# exchange types
EXCHANGE_TYPE_FANOUT = 'fanout'
EXCHANGE_TYPE_TOPIC = 'topic'

# on-disk only queue mode
QUEUE_MODE_LAZY = "lazy"

AnyFunc = Callable[..., Any]
F = TypeVar('F')
Decorator = Callable[[F], F]
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

    @overload
    def handler(self, *, bind: bool = False) -> Decorator:
        ...

    @overload
    def handler(self, func: F) -> F:
        ...

    def handler(self, func: F = None, *, bind: bool = False
                ) -> Union[Decorator, F]: ...


class EventsCelery(Celery):
    task_cls = task.EventHandler

    def __init__(self, main: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(main, *args, **kwargs)
        self.on_after_finalize.connect(self._register_retry_queues)
        self.on_after_finalize.connect(self._register_archived_queue)
        self.on_after_finalize.connect(self._generate_task_queues)
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

    @property
    def recover_exchange_name(self) -> str:
        """ Name of recover exchange name."""
        return f'{self.main}:recover'

    @property
    def archived_exchange_name(self) -> str:
        """
        Name of exchange and queue for archived events.
        """
        return f'{self.main}:archived'

    def get_retry_exchange_name(self, retry: int = 0) -> str:
        """
        Name of DLX for event queues and retry exchange prefix.
        Also used as retry queues prefix.
        """
        default_name = f"{self.main}:retry"
        if not retry:
            return default_name
        return f"{default_name}.{retry}"

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

        Each handler has it's own queue named as `service_name.event_name`.
        This queue is bound to `events` exchange with event routing key and
        receives initial events from it. If broker-side retry is enabled, queue
        is also bound to `recover` exchange with same routing key, from which
        it receives retried events.
        Also `recover` defines `dead-letter-exchange` which re-routes rejected
        messages to `retry` queue with 1 second duration (in case of failed
        republish).

        Last thing is `archived` echange/queue pair. `archived` queue has
        limited message ttl and queue-length. When a message exceeds max
        retries it is republished to `archived` queue.

        `events` -> (event routing key) -> `demo.my_event` -> Celery worker
            | (message is rejected by worker and message rerouted via DLX)
            V
        `demo:retry` -> `demo:retry` -> ... 1 second elapsed
            | (message ttl expires )
            V
        `recover` -> (routing key) -> `demo.my_event_queue` - > Celery worker
            | (MaxTaskRetriesExceeded)
            V
        `archived` -> `demo.archived`

        """
        queues = self.conf.task_queues or []
        if queues:
            return
        exchange = Exchange(
            name=self.conf.task_default_exchange,
            type=self.conf.task_default_exchange_type)

        if defaults.AMQP_EVENTS_MAX_RETRIES > 0:
            # Bind same routing key to "recover" exchange if broker-side delays
            # are enabled
            recover = Exchange(
                name=self.recover_exchange_name,
                type=EXCHANGE_TYPE_TOPIC)
        else:
            recover = None

        queue_arguments = None
        if defaults.AMQP_EVENTS_MAX_RETRIES:
            queue_arguments = {
                X_DEAD_LETTER_EXCHANGE: self.get_retry_exchange_name(),
            }

        for name in self._handlers:
            bindings = [binding(exchange=exchange, routing_key=name)]
            if recover:
                bindings.append(binding(exchange=recover, routing_key=name))
            queue = Queue(
                name=f'{self.main}.{name}',
                bindings=bindings,
                queue_arguments=queue_arguments
            )
            queues.append(queue)
        self.conf.task_queues = queues

    def _register_retry_queues(self, **_: Any) -> None:
        """
        Initializes a set of AMQP primitives to implement broker-based delays.

        Declares an exchange/queue pair for each delay stage defined by
        `amqp_events.defaults:AMQP_EVENTS_MAX_RETRIES`.
        Each exchange has a single bound queue; when task needs to be delayed,
        it's re-published to new exchange preserving initial routing_key.
        A queue for each exchange has `message-ttl` set to a power of 2. After
        message is expired, it is re-routed by broker to `dead-letter-exchange`
        named `recover`. All queues defined for event handlers are also bound to
        this exchange with same routing key, thus each message after a retry
        appears in same incoming queue.

        `events` -> `demo.my_event_queue` -> Celery worker (retry)
            | (publishes new message on retry)
            V
        `demo:retry.N` -> `demo:retry.N`
            | (message ttl expires )
            V
        `recover` -> (routing key) -> `demo.my_event_queue` - > Celery worker
        """
        channel = self.broker_connection().default_channel
        for retry in range(defaults.AMQP_EVENTS_MAX_RETRIES):
            name = self.get_retry_exchange_name(retry)
            retry_exchange = Exchange(name=name, type=EXCHANGE_TYPE_FANOUT)
            retry_queue = Queue(
                name=name,
                exchange=retry_exchange,
                queue_arguments={
                    X_MESSAGE_TTL: 2 ** retry * 1000,  # ms
                    X_DEAD_LETTER_EXCHANGE: self.recover_exchange_name,
                }
            )
            retry_queue.declare(channel=channel)
            retry_queue.maybe_bind(channel=channel)

    def _register_archived_queue(self, **_: Any) -> None:
        """
        Registers an exchange and a queue for archived messages.
        """
        max_ttl = defaults.AMQP_EVENTS_ARCHIVED_MESSAGE_TTL
        max_len = defaults.AMQP_EVENTS_ARCHIVED_QUEUE_LENGTH
        if not (max_ttl or max_len):
            # archived exchange is disabled
            return
        archived = Exchange(name=self.archived_exchange_name,
                            type=EXCHANGE_TYPE_FANOUT)
        channel = self.broker_connection().default_channel
        archived_queue = Queue(
            name=self.archived_exchange_name,
            exchange=archived,
            queue_arguments={
                X_MESSAGE_TTL: max_ttl,
                X_MAX_LENGTH: max_len,
                X_QUEUE_MODE: QUEUE_MODE_LAZY
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
        s = canvas.Signature(
            args=args,
            kwargs=kwargs,
            task=self.name,
            app=self.app,
            task_type=self.app.Task,
            routing_key=self.name)
        result: AsyncResult = s.apply_async()
        return result.id

    def handler(self,
                func: Optional[AnyFunc] = None,
                bind: bool = False
                ) -> Union[Callable[[AnyFunc], AnyFunc], AnyFunc]:
        if func and callable(func):
            return self.app.handler(self.name, bind=bind)(func)
        return self.app.handler(self.name, bind=bind)


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
