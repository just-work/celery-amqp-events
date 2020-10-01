import math
import random
from functools import wraps
from unittest import TestCase, mock
from unittest.mock import MagicMock

from celery.app.task import Task
from celery.exceptions import Reject
from kombu import Queue, Exchange

from amqp_events import defaults
from amqp_events.celery import EventsCelery
from demo import celery, events


def disable_patcher(name: str):
    """ Декоратор для временного отключения unittest.mock.patch."""

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            patcher = getattr(self, name)
            patcher.stop()
            try:
                return func(self, *args, **kwargs)
            finally:
                patcher.start()

        return wrapper

    return decorator


# noinspection PyPep8Naming
class override_defaults:
    """ Allows to easily patch `defaults` package in any module."""

    def __init__(self, module_name, **new_values):
        super().__init__()
        self.patchers = []
        self.module_name = module_name
        self.new_values = new_values

    def enable(self):
        for k, v in self.new_values.items():
            p = mock.patch(f'{self.module_name}.defaults.{k}',
                           new_callable=mock.PropertyMock(return_value=v))
            p.start()
            self.patchers.append(p)

    def disable(self):
        for p in self.patchers:
            p.stop()
        self.patchers.clear()

    def __enter__(self):
        return self.enable()

    def __exit__(self, exc_type, exc_value, traceback):
        self.disable()

    def decorate_class(self, cls):
        if issubclass(cls, TestCase):
            decorated_setUp = cls.setUp
            decorated_tearDown = cls.tearDown

            def setUp(inner_self):
                self.enable()
                try:
                    decorated_setUp(inner_self)
                except Exception:
                    self.disable()
                    raise

            def tearDown(inner_self):
                decorated_tearDown(inner_self)
                self.disable()

            cls.setUp = setUp
            cls.tearDown = tearDown
            return cls
        raise TypeError('Can only decorate subclasses of unittest.TestCase')

    def decorate_callable(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return inner

    def __call__(self, decorated):
        if isinstance(decorated, type):
            return self.decorate_class(decorated)
        elif callable(decorated):
            return self.decorate_callable(decorated)
        raise TypeError('Cannot decorate object of type %s' % type(decorated))


class EventsCeleryTestCase(TestCase):
    """ Checks the EventsCelery methods."""

    def setUp(self) -> None:
        self.app = EventsCelery("test")
        self.app.conf.broker_url = 'in-memory:///'
        self.app.conf.task_default_exchange_type = 'topic'
        self.app.conf.task_always_eager = True
        self.my_handler_func = mock.MagicMock(__name__='my_handler_func')
        self.event_name = f'my.event.{random.randint(1, 100000000)}'
        self.event_function_mock = mock.MagicMock()
        self.my_event = self.app.event(self.event_name)(
            self.event_function_mock)
        self.apply_async_patcher = mock.patch('celery.canvas.Signature')
        self.apply_async_mock: mock.MagicMock = self.apply_async_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.apply_async_patcher.stop()

    @property
    def task(self):
        return self.app.tasks[self.my_event.name]

    def test_send_event(self):
        """
        Event is sent to broker via event function call.
        """
        self.my_event('arg', kw='arg')

        self.event_function_mock.assert_called_once_with(
            'arg', kw='arg')
        self.apply_async_mock.assert_called_once_with(
            args=('arg',),
            kwargs={'kw': 'arg'},
            task=self.event_name,
            app=self.app,
            task_type=self.app.Task,
            routing_key=self.event_name)
        self.apply_async_mock.return_value.apply_async.assert_called_once_with()

    def test_event_validation(self):
        """
        Event function body is called before event is sent to a broker.

        This is useful for validation purposes.
        """
        self.event_function_mock.side_effect = Exception("test error")
        self.assertRaises(Exception, self.my_event)
        self.apply_async_mock.assert_not_called()

    def test_register_handler_function(self):
        """
        A function may be registered as event handler.
        """
        res = self.app.handler(self.event_name)(self.my_handler_func)

        self.assertIs(self.my_handler_func, res)

        sentinel = mock.sentinel

        self.task.apply(args=(sentinel,), throw=True)

        self.my_handler_func.assert_called_once_with(sentinel)

    def test_register_bound_handler_function(self):
        """
        A task argument is passed to a registered event handler with `bind`
        flag set up.
        """
        m = self.my_handler_func

        def bound_handler(task, *args, **kwargs):
            return m(task, *args, **kwargs)

        res = self.app.handler(self.event_name, bind=True)(bound_handler)

        self.assertIs(bound_handler, res)

        sentinel = mock.sentinel

        self.task.apply(args=(sentinel,), throw=True)

        self.my_handler_func.assert_called_once_with(self.task, sentinel)

    def test_register_task_class_as_handler(self):
        """
        An event handler could be a class derived from `EventsCelery.Task`.
        """
        m = self.my_handler_func

        class MyTaskHandler(self.app.Task):
            def run(self, *args, **kwargs):
                return m(self, *args, **kwargs)

        h = self.app.handler(self.event_name)(MyTaskHandler)

        self.assertIs(MyTaskHandler, h)
        self.assertIsInstance(self.task, MyTaskHandler)

        sentinel = mock.sentinel

        self.task.apply(args=(sentinel,), throw=True)

        self.my_handler_func.assert_called_once_with(self.task, sentinel)

    def test_register_any_task_as_handler(self):
        """
        A handle may not inherit from `EventsCelery.Task` class.
        """
        m = self.my_handler_func

        class MyTaskHandler(Task):
            def run(self, *args, **kwargs):
                return m(self, *args, **kwargs)

        h = self.app.handler(self.event_name)(MyTaskHandler)

        self.assertIs(MyTaskHandler, h)
        self.assertIsInstance(self.task, MyTaskHandler)
        self.assertIsInstance(self.task, self.app.Task)

        sentinel = mock.sentinel

        self.task.apply(args=(sentinel,), throw=True)

        self.my_handler_func.assert_called_once_with(self.task, sentinel)

    def test_autocreate_queue_for_handler(self):
        """
        If no `task_queues` defined, a queue for each event with handler is
        created.
        """
        self.app.handler(self.event_name)(self.my_handler_func)

        self.app.on_after_finalize.send(self.app)

        queues = self.app.conf.task_queues
        self.assertEqual(len(queues), 1)
        queue = queues[0]
        self.assertIsInstance(queue, Queue)
        self.assertEqual(queue.name, f"{self.app.main}.{self.event_name}")
        # multiple exchange bindings
        self.assertIsNone(queue.exchange)
        self.assertEqual(len(queue.bindings), 2)

        bindings = {b.exchange.name: b for b in queue.bindings}
        b0 = bindings[self.app.conf.task_default_exchange]
        b1 = bindings[self.app.recover_exchange_name]
        for b in b0, b1:
            self.assertIsInstance(b.exchange, Exchange)
            self.assertEqual(b.exchange.type,
                             self.app.conf.task_default_exchange_type)
            self.assertEqual(b.routing_key, self.event_name)

        retry_exchange = self.app.get_retry_exchange_name()
        expected = {'x-dead-letter-exchange': retry_exchange}
        self.assertDictEqual(getattr(queue, 'queue_arguments'), expected)

    def test_skip_queue_auto_creation_if_already_defined(self):
        """
        If `task_queues` is set up, no automatic queues are created.
        """
        expected = [Queue("my.queue")]
        self.app.conf.task_queues = expected.copy()
        self.app.handler(self.event_name)(self.my_handler_func)

        self.app.on_after_finalize.send(self.app)

        queues = self.app.conf.task_queues
        self.assertListEqual(expected, queues)

    def test_prevent_double_handler_registration(self):
        """
        Every event has only one event handler.
        """
        decorator = self.app.handler(self.event_name)
        decorator(self.my_handler_func)

        self.assertRaises(RuntimeError, decorator, mock.MagicMock())


class HandlerTaskTestCase(TestCase):
    def setUp(self) -> None:
        self.app = celery.app
        self.app.loader.import_default_modules()
        self.request = MagicMock(
            retries=0,
            called_directly=False,
            is_eager=False
        )
        self.task = self.app.tasks[events.event_occured.name]
        self.retry_patcher = mock.patch('celery.app.task.Task.retry')
        self.retry_mock = self.retry_patcher.start()
        self.request_patcher = mock.patch('celery.app.task.Task.request',
                                          new_callable=mock.PropertyMock(
                                              return_value=self.request))
        self.request_patcher.start()

    def tearDown(self) -> None:
        self.retry_patcher.stop()
        self.request_patcher.stop()

    def run_task(self, *args, **kwargs):
        return self.task.apply(args=args, kwargs=kwargs, throw=True)

    def test_retry_without_countdown(self):
        """
        When a task is retried without countdown, it is set to 1 second.
        """
        max_retries = defaults.AMQP_EVENTS_MAX_RETRIES
        # ensure that retries count doesn't affect default countdown
        self.request.retries = random.randrange(max_retries)

        self.run_task('retry')

        args = kwargs = exc = eta = None
        throw = True
        countdown = 1.0
        exchange = self.app.get_retry_exchange_name(0)
        self.retry_mock.assert_called_once_with(
            args, kwargs, exc, throw, eta, countdown, max_retries,
            exchange=exchange)

    def test_retry_with_countdown(self):
        """
        When a task is retries with countdown, it is routed so an exchange that
        corresponds to this countdown value.
        """
        max_retries = defaults.AMQP_EVENTS_MAX_RETRIES
        countdown = random.randint(1, 2 ** max_retries - 1)

        self.run_task('retry', countdown=countdown)

        args = kwargs = exc = eta = None
        throw = True
        retry_number = math.floor(math.log2(countdown))
        exchange = self.app.get_retry_exchange_name(retry_number)

        self.retry_mock.assert_called_once_with(
            args, kwargs, exc, throw, eta, countdown, max_retries,
            exchange=exchange)

    def test_countdown_max_limit(self):
        """
        When countdown exceeds a value computed from AMQP_EVENTS_MAX_RETRIES,
        last retry exchange is used.
        """
        max_retries = defaults.AMQP_EVENTS_MAX_RETRIES
        countdown = 2 ** max_retries + 1

        self.run_task('retry', countdown=countdown)

        args = kwargs = exc = eta = None
        throw = True
        exchange = self.app.get_retry_exchange_name(max_retries)

        self.retry_mock.assert_called_once_with(
            args, kwargs, exc, throw, eta, countdown, max_retries,
            exchange=exchange)

    def test_archive_on_max_retries_exceeded(self):
        """
        When max retried exceeds allowed limit, task is routed to archive queue.
        """
        max_retries = defaults.AMQP_EVENTS_MAX_RETRIES
        self.request.retries = max_retries

        self.run_task('retry')

        args = kwargs = exc = eta = None
        throw = True
        countdown = None
        exchange = self.app.archived_exchange_name

        self.retry_mock.assert_called_once_with(
            args, kwargs, exc, throw, eta, countdown, max_retries + 1,
            exchange=exchange)

    @disable_patcher('retry_patcher')
    def test_reject_on_retry_failure(self):
        """
        If retrying event fails, original event is rejected without requeue.

        Instead it is sent by broker to retry exchange defined by
        `x-dead-letter-exchange` attribute for event handler queue.
        """
        err = ConnectionError("test")
        with mock.patch('celery.canvas.Signature.apply_async',
                        side_effect=err):
            res = self.run_task('retry')
        self.assertIsInstance(res.result, Reject)
        self.assertFalse(res.result.requeue)
        self.assertEqual(res.result.reason, err)

    def test_autoretry_for(self):
        """
        For an unhandled exceptions event is retried via retry exchange.

        This is configured with `EventHandler.autoretry_for=(Exception,)`
        """

        class RetryException(Exception):
            pass

        self.retry_mock.return_value = RetryException()
        exc = Exception('test error')

        self.assertRaises(RetryException, self.run_task, exc)
        args = kwargs = eta = None
        throw = True
        countdown = 1
        max_retries = defaults.AMQP_EVENTS_MAX_RETRIES
        exchange = self.app.get_retry_exchange_name(0)

        self.retry_mock.assert_called_once_with(
            args, kwargs, exc, throw, eta, countdown, max_retries,
            exchange=exchange)
